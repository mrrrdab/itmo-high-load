package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.channels.Channel
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.OngoingWindow
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import kotlin.math.max


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        const val MAX_RETRIES = 5
        val RETRIABLE_ERROR_CODES = listOf(429, 500, 502, 503, 504)
        const val MAX_IDLE_CONNECTIONS = 300
        const val CHANNEL_CAPACITY = 3_000
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime.toMillis()
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .connectionPool(ConnectionPool(MAX_IDLE_CONNECTIONS, 5, java.util.concurrent.TimeUnit.MINUTES))
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .readTimeout(Duration.ofSeconds(30))
        .writeTimeout(Duration.ofSeconds(30))
        .callTimeout(Duration.ofMillis(requestAverageProcessingTime * 2))
        .retryOnConnectionFailure(true)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)
    
    private val paymentScope = CoroutineScope(
        Dispatchers.IO + SupervisorJob() + 
        CoroutineExceptionHandler { _, exception ->
            logger.error("[$accountName] Uncaught exception in payment coroutine", exception)
        }
    )

    private val paymentChannel = Channel<PaymentRequest>(CHANNEL_CAPACITY)
    
    init {
        repeat(parallelRequests) {
            paymentScope.launch {
                for (request in paymentChannel) {
                    try {
                        processPaymentRequest(request)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Error processing payment ${request.paymentId}", e)
                    }
                }
            }
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        
        if (!paymentChannel.trySend(PaymentRequest(paymentId, amount, paymentStartedAt, deadline)).isSuccess) {
            logger.error("[$accountName] Request queue is full, rejecting payment $paymentId")

            val rejectionTransactionId = UUID.randomUUID()
            paymentESService.update(paymentId) {
                it.logSubmission(success = false, rejectionTransactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                it.logProcessing(false, now(), rejectionTransactionId, reason = "Request queue full")
            }
            return
        }
    }

    private suspend fun processPaymentRequest(request: PaymentRequest) {
        val (paymentId, amount, paymentStartedAt, deadline) = request
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Processing payment $paymentId, txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val httpRequest = buildRequest(paymentId, amount, transactionId, requestAverageProcessingTime * 2)

        var curRetry = 0
        val maxRetries = max(((deadline - paymentStartedAt) / requestAverageProcessingTime).toInt(), MAX_RETRIES)
        var delay = requestAverageProcessingTime / 8

        while (curRetry < maxRetries) {
            try {
                if (checkDeadlineExceeded(paymentId, transactionId, deadline, estimatedProcessingTime = requestAverageProcessingTime)) {
                    return
                }

                if (!rateLimiter.tick()) {
                    logger.debug("[$accountName] Rate limit hit, waiting...")
                    while (!rateLimiter.tick()) {
                        delay(1)
                    }
                    logger.debug("[$accountName] Rate limit passed")
                }

                if (!ongoingWindow.tryAcquire(deadline)) {
                    logger.error("[$accountName] Failed to acquire resource for payment $paymentId - deadline approaching")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Resource acquisition timeout")
                    }
                    return
                }

                val response = executeRequest(httpRequest, transactionId, paymentId)
                
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${response.result}, message: ${response.message}")
                
                paymentESService.update(paymentId) {
                    it.logProcessing(response.result, now(), transactionId, reason = response.message)
                }

                if (response.result) {
                    return
                }

                if (response.httpResponseCode != null && response.httpResponseCode !in RETRIABLE_ERROR_CODES) {
                    logger.warn("[$accountName] Non-retriable error for $paymentId, stopping retries.")
                    return
                }

                
            } catch (e: SocketTimeoutException) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message ?: "Unknown error")
                }
                return
            }

            logger.warn("[$accountName] Retrying payment $paymentId in $delay ms (attempt $curRetry/$maxRetries).")
            delay(delay)

            delay *= 2
            curRetry++
        }
        
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Max retries exceeded")
        }
    }

    private suspend fun executeRequest(
        request: Request,
        transactionId: UUID,
        paymentId: UUID
    ): ExternalSysResponse {
        return suspendCancellableCoroutine { continuation ->
            client.newCall(request).enqueue(object : Callback {
                override fun onResponse(call: Call, response: Response) {
                    try {
                        response.use {
                            val responseBody = it.body?.string()
                            val externalSysResponse = mapper.readValue(responseBody, ExternalSysResponse::class.java)
                            val responseWithCode = externalSysResponse.copy(httpResponseCode = it.code)
                            continuation.resume(responseWithCode)
                        }
                    } catch (e: Exception) {
                        logger.error("[$accountName] Error processing response for txId: $transactionId, payment: $paymentId", e)
                        continuation.resume(ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message, response.code))
                    }
                }

                override fun onFailure(call: Call, e: IOException) {
                    logger.error("[$accountName] Request failed for txId: $transactionId, payment: $paymentId", e)
                    continuation.resumeWithException(e)
                }
            })

            continuation.invokeOnCancellation {
                try {
                    client.dispatcher.executorService.execute {
                        client.dispatcher.cancelAll()
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Error cancelling requests", e)
                }
            }
        }
    }

    private fun checkDeadlineExceeded(paymentId: UUID, transactionId: UUID, deadline: Long, estimatedProcessingTime: Long): Boolean {
        if (now() + estimatedProcessingTime >= deadline) {
            logger.error("[$accountName] Deadline exceeded, payment $paymentId canceled.")

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
            }

            return true
        }

        return false
    }

    private fun buildRequest(paymentId: UUID, amount: Int, transactionId: UUID, timeout: Long): Request {
        return Request.Builder().run {
            url(
                "http://localhost:1234/external/process?" +
                "serviceName=${serviceName}&" +
                "accountName=${accountName}&" +
                "transactionId=$transactionId&" +
                "paymentId=$paymentId&" +
                "amount=$amount&" +
                "timeout=${Duration.ofMillis(timeout)}"
            )
            post(emptyBody)
        }.build()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
    
    fun shutdown() {
        paymentChannel.close()
        paymentScope.cancel()
        client.dispatcher.executorService.shutdown()
        client.connectionPool.evictAll()
    }
}

data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long
)

public fun now() = System.currentTimeMillis()