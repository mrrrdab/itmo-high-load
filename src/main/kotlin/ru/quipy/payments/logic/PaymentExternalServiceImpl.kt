package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.OngoingWindow
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        val RETRIABLE_ERROR_CODES = listOf(429, 500, 502, 503, 504)

        val MAX_CORE_THREAD_COUNT = 100
        val MAX_THREAD_COUNT = 500
        val WORK_QUEUE_SIZE = 5000
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime.toMillis()
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().callTimeout(Duration.ofMillis(requestAverageProcessingTime * 2)).build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val threadCount = ((rateLimitPerSec / (1000.0 / requestAverageProcessingTime)).toInt()).coerceAtLeast(1).coerceAtMost(MAX_CORE_THREAD_COUNT)
    private val executorService = ThreadPoolExecutor(
        threadCount,
        threadCount,
        0,
        TimeUnit.MINUTES,
        LinkedBlockingQueue<Runnable>(WORK_QUEUE_SIZE),
        Executors.defaultThreadFactory(),
        ThreadPoolExecutor.DiscardPolicy()
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        executorService.submit {
            val transactionId = UUID.randomUUID()
            val idempotencyKey = UUID.randomUUID().toString()
            logger.info("[$accountName] Submit for $paymentId, txId: $transactionId, idKey: $idempotencyKey")

            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val request = buildRequest(paymentId, amount, transactionId, idempotencyKey, requestAverageProcessingTime * 2)

            var curRetry = 0
            val maxRetries = ((deadline - paymentStartedAt) / requestAverageProcessingTime).toInt()
            var delay = requestAverageProcessingTime / 8

            while (curRetry < maxRetries) {
                try {
                    val body = executeHedgedRequest(client, request, mapper, accountName, transactionId, paymentId, requestAverageProcessingTime * 2, deadline, requestAverageProcessingTime * 2)

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
        
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    if (body.result) {
                        return@submit
                    }

                    if (body.httpResponseCode != null && body.httpResponseCode !in RETRIABLE_ERROR_CODES) {
                        logger.warn("[$accountName] Non-retriable error for $paymentId, stopping retries.")
                        return@submit
                    }
                } catch (e: SocketTimeoutException) {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                } catch (e: Exception) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    return@submit
                } finally {
                    ongoingWindow.release()
                }

                logger.warn("[$accountName] Retrying payment $paymentId in $delay ms (attempt $curRetry/$maxRetries).")
                Thread.sleep(delay)

                delay *= 2
                curRetry++
            }
        }
    }

    private fun executeHedgedRequest(
        client: OkHttpClient,
        request: Request,
        mapper: ObjectMapper,
        accountName: String,
        transactionId: UUID,
        paymentId: UUID,
        timeout: Long,
        deadline: Long,
        estimatedProcessingTime: Long
    ): ExternalSysResponse {
        val executor = Executors.newFixedThreadPool(2)
        
        try {
            val mainRequest = executor.submit<ExternalSysResponse> {
                executeRequest(client, request, mapper, accountName, transactionId, paymentId, deadline, estimatedProcessingTime)
            }

            var hedgedRequest: Future<ExternalSysResponse>? = null

            try {
                return mainRequest.get(timeout, TimeUnit.MILLISECONDS) as ExternalSysResponse
            } catch (e: TimeoutException) {
                logger.warn("[$accountName] Primary request for $paymentId timed out. Sending hedged request.")

                hedgedRequest = executor.submit<ExternalSysResponse> {
                    executeRequest(client, request, mapper, accountName, transactionId, paymentId, deadline, estimatedProcessingTime)
                }

                return CompletableFuture.anyOf(
                    CompletableFuture.supplyAsync { mainRequest.get() },
                    CompletableFuture.supplyAsync { hedgedRequest.get() }
                ).get() as ExternalSysResponse
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            return ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }
    }

    private fun executeRequest(
        client: OkHttpClient,
        request: Request,
        mapper: ObjectMapper,
        accountName: String,
        transactionId: UUID,
        paymentId: UUID,
        deadline: Long,
        estimatedProcessingTime: Long
    ): ExternalSysResponse {
        if (!ongoingWindow.tryAcquire(deadline)) {
            logger.error("[$accountName] Deadline exceeded, payment $paymentId canceled.")

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
            }

            return ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Deadline exceeded.")
        }

        if (!rateLimiter.tick()) {
            logger.warn("[$accountName] Rate limit exceeded, payment $paymentId delayed.")
            rateLimiter.tickBlocking()
        }

        if (checkDeadlineExceeded(paymentId, transactionId, deadline, estimatedProcessingTime)) {
            return ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Deadline exceeded.")
        }

        try {
            return client.newCall(request).execute().use { response ->
                try {
                    val externalSysResponse = mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    return externalSysResponse.copy(httpResponseCode = response.code)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    return ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message, response.code)
                }
            }
        } finally {
            ongoingWindow.release()
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

    private fun buildRequest(paymentId: UUID, amount: Int, transactionId: UUID, idempotencyKey: String, timeout: Long): Request {
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
            addHeader("x-idempotency-key", idempotencyKey)
            post(emptyBody)
        }.build()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()