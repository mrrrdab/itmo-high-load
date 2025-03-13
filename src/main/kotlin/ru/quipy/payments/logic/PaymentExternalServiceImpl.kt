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
import kotlin.math.min
import kotlin.math.max


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId.")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        var curRetry = 0
        val maxRetries = (deadline - paymentStartedAt) / requestAverageProcessingTime.toMillis()
        var delay = requestAverageProcessingTime.toMillis() / 4

        while (curRetry < maxRetries) {
            try {
                if (!ongoingWindow.tryAcquire(deadline - now())) {
                    logger.error("[$accountName] Deadline exceeded, payment $paymentId canceled.")

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                    }

                    return
                }

                if (!rateLimiter.tick()) {
                    logger.warn("[$accountName] Rate limit exceeded, payment $paymentId delayed.")
                    rateLimiter.tickBlocking()
                }

                if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                    logger.error("[$accountName] Deadline exceeded, payment $paymentId canceled.")

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                    }
    
                    return
                }

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }
    
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
    
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    if (body.result) {
                        return
                    }

                    if (response.code !in listOf(429, 500, 502, 503, 504)) {
                        logger.warn("[$accountName] Non-retriable error for $paymentId, stopping retries.")
                        return
                    }
                }
            } catch (e: SocketTimeoutException) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                return
            } finally {
                ongoingWindow.release()
            }

            logger.warn("[$accountName] Retrying payment $paymentId in $delay ms (attempt $curRetry/$maxRetries).")
            Thread.sleep(delay)
            
            delay = delay * 2
            curRetry++
        }

        logger.error("[$accountName] Payment $paymentId failed after $maxRetries attempts.")

        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Max retries reached or deadline exceeded.")
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()