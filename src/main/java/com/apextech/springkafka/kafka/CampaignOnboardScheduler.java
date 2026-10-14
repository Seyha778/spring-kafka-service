package com.apextech.springkafka.kafka;

import com.apextech.springkafka.config.properties.CampaignOnboardProperties;
import com.apextech.springkafka.service.impl.CashbackOnboardBatchClaimServiceImpl;
import com.apextech.springkafka.service.impl.CashbackOnboardNotifyServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class CampaignOnboardScheduler {

    private static final String LOCK_KEY = ":CAMPAIGN:ONBOARD_SCHEDULE:LOCK";
    private static final Duration LOCK_TTL = Duration.ofMinutes(10);

    @Value("${SPRING_PROFILES_ACTIVE:LOCAL}")
    private String env;

    private final CashbackOnboardBatchClaimServiceImpl batchClaimService;
    private final RedisTemplate<String, String> redisTemplate;
    private final CampaignOnboardProperties properties;

    public void runDailyCampaign() {
        String runId = UUID.randomUUID().toString();

        String lockKey = env.toUpperCase() + LOCK_KEY;
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(lockKey, runId, LOCK_TTL);
        if (!Boolean.TRUE.equals(acquired)) {
            log.info("Campaign run skipped - another instance is already running it. runId={}", runId);
            return;
        }

        try {
            log.info("Campaign run started. runId={}", runId);

            int batchesProcessed = 0;
            int totalClaimed = 0;

            while (batchesProcessed < properties.maxBatchesPerRun()) {
                int claimed = batchClaimService.claimAndPublishNextBatch(runId);
                totalClaimed += claimed;
                batchesProcessed++;

                if (claimed < properties.batchSize()) {
                    break;
                }
            }

            if (totalClaimed == 0) {
                log.info("Campaign run had nothing to process - no PENDING rows found today. runId={}", runId);
                return;
            }

            // Store the total number of claimed records for this run in Redis.
            // The consumer uses this value to determine when all claimed records have been processed.
            redisTemplate
                    .opsForValue()
                    .set(
                            env.toUpperCase()
                                    + CashbackOnboardNotifyServiceImpl.KEY_PREFIX
                                    + runId
                                    + CashbackOnboardNotifyServiceImpl.KEY_TOTAL_SUFFIX,
                            String.valueOf(totalClaimed));

            log.info(
                    "Campaign onboard claim/publish phase finished (rows now queued in Kafka - NOT yet credited; crediting happens asynchronously via the consumer). runId={}, batches={}, totalClaimed={}",
                    runId,
                    batchesProcessed,
                    totalClaimed);
        } finally {
            redisTemplate.delete(lockKey);
        }
    }
}
