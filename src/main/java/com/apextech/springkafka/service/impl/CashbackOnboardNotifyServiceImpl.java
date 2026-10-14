package com.apextech.springkafka.service.impl;

import com.apextech.springkafka.constant.TxnStatus;
import com.apextech.springkafka.entity.CashbackOnboardTxnRequest;
import com.apextech.springkafka.repository.CashbackOnboardTxnRequestRepository;
import com.apextech.springkafka.service.CashbackOnboardNotifyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class CashbackOnboardNotifyServiceImpl implements CashbackOnboardNotifyService {

    public static final String KEY_PREFIX = ":CAMPAIGN_ONBOARD_PROGRESS:";
    public static final String KEY_TOTAL_SUFFIX = "_TOTAL:CACHE";
    public static final String KEY_ATTEMPTED_SUFFIX = "_ATTEMPTED:CACHE";

    @Value("${SPRING_PROFILES_ACTIVE:LOCAL}")
    private String env;

    private final RedisTemplate<String, String> redisTemplate;
    private final CashbackOnboardTxnRequestRepository repository;

    @Override
    public void recordAttemptAndNotifyIfComplete(String runId) {
        String attemptedKey = attemptedKey(runId);
        Long attempted = redisTemplate.opsForValue().increment(attemptedKey);
        redisTemplate.expire(attemptedKey, ttl());

        String totalValue = redisTemplate.opsForValue().get(totalKey(runId));

        if (log.isDebugEnabled()) {
            log.debug(
                    "Checking run completion: runId={}, totalKey={}, totalValue={}, attempted={}",
                    runId,
                    totalKey(runId),
                    totalValue,
                    attempted);
        }

        if (!StringUtils.hasText(totalValue) || Objects.isNull(attempted) || attempted < Long.parseLong(totalValue)) {
            return;
        }

        List<CashbackOnboardTxnRequest> list = repository.findAllByRunId(runId);
        Map<TxnStatus, Long> statusCounts = list.stream()
                .collect(Collectors.groupingBy(CashbackOnboardTxnRequest::getStatus, Collectors.counting()));

        log.info(
                "Push notify CAMPAIGN ONBOARD SUMMARY: runId={}, total={}, statusCounts={}",
                runId,
                list.size(),
                statusCounts);
    }

    private Duration ttl() {
        return Duration.ofDays(1);
    }

    private String attemptedKey(String runId) {
        return env.toUpperCase() + KEY_PREFIX + runId + KEY_ATTEMPTED_SUFFIX;
    }

    private String totalKey(String runId) {
        return env.toUpperCase() + KEY_PREFIX + runId + KEY_TOTAL_SUFFIX;
    }
}
