package com.apextech.springkafka.service;

import com.apextech.springkafka.config.properties.RateLimiterProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class RateLimiterService {

    private static final String RATE_LIMIT_SCOPE = "cashback-onboard-txn";

    private final RedisTemplate<String, String> redisTemplate;
    private final DefaultRedisScript<Long> rateLimiterScript;
    private final RateLimiterProperties properties;

    /**
     * Executes the sliding-window Lua script (ZCARD + ZADD + ZRANGE/ZREMRANGEBYSCORE + EXPIRE)
     * to enforce {@code limitPerWindow} requests per {@code windowMillis}, scoped per campaign.
     *
     * @return true if the request is permitted, false if it should be rejected (rate limit exceeded)
     */
    public boolean tryAcquire() {
        String key = properties.keyPrefix() + RATE_LIMIT_SCOPE;
        long now = System.currentTimeMillis();
        String member = now + ":" + UUID.randomUUID();

        Long result = redisTemplate.execute(
                rateLimiterScript,
                List.of(key),
                String.valueOf(now),
                String.valueOf(properties.windowMillis()),
                String.valueOf(properties.limitPerWindow()),
                member);

        boolean permitted = result == 1L;
        if (!permitted) {
            log.debug("Rate limit exceeded. key={}", key);
        }
        return permitted;
    }

    public long retryBackoffMillis() {
        return properties.retryBackoffMillis();
    }
}
