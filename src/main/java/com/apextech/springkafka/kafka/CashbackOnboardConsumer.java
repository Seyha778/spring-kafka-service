package com.apextech.springkafka.kafka;

import com.apextech.springkafka.dto.CashbackTxnEvent;
import com.apextech.springkafka.service.CashbackOnboardNotifyService;
import com.apextech.springkafka.service.RateLimiterService;
import com.apextech.springkafka.service.impl.CashbackOnboardProcessServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

@Component
@RequiredArgsConstructor
@Slf4j
public class CashbackOnboardConsumer {

    public static final String LISTENER_ID = "cashback-txn-listener";

    private final RateLimiterService rateLimiterService;
    private final CashbackOnboardProcessServiceImpl processingService;
    private final CashbackOnboardNotifyService notificationService;
    private final KafkaListenerEndpointRegistry registry;
    private final ThreadPoolTaskScheduler rateLimiterTaskScheduler;

    @KafkaListener(
            id = LISTENER_ID,
            topics = "${app.campaign.onboard.topic}",
            groupId = "${app.campaign.onboard.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, CashbackTxnEvent> record, Acknowledgment acknowledgment) {

        CashbackTxnEvent event = record.value();

        // KP -> VK: Lua rate-limit script (ZCARD + ZADD + ZRANGE + EXPIRE)
        // No more per-campaign scoping - a single global 1 TPS budget applies.
        boolean permitted = rateLimiterService.tryAcquire();

        if (!permitted) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Rate limit exceeded for customerId={} id={} - nacking for redelivery",
                        event.getCustomerId(),
                        event.getId());
            }
            pauseAndScheduleResume();
            acknowledgment.nack(Duration.ZERO);
            return;
        }

        try {
            processingService.process(event);
        } catch (Exception ex) {
            log.error("Unhandled error processing customerId={} id={}", event.getCustomerId(), event.getId(), ex);
            throw ex;
        } finally {
            acknowledgment.acknowledge();
            // Track every record attempt, including failed processing, and trigger the completion
            // notification once all records claimed for this run have been attempted.
            notificationService.recordAttemptAndNotifyIfComplete(event.getRunId());
        }
    }

    private void pauseAndScheduleResume() {
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        if (Objects.isNull(container)) {
            log.warn("Listener container '{}' not found - cannot pause", LISTENER_ID);
            return;
        }

        if (!container.isContainerPaused()) {
            container.pause();
            long delayMillis = rateLimiterService.retryBackoffMillis();
            rateLimiterTaskScheduler.schedule(
                    () -> {
                        log.debug("Resuming listener container '{}'", LISTENER_ID);
                        container.resume();
                    },
                    Instant.now().plusMillis(delayMillis));
        }
    }
}
