package com.apextech.springkafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Configuration
public class RedisConfig {

    /**
     * Loads lua/rate_limiter.lua (ZCARD + ZADD/ZREMRANGEBYSCORE + EXPIRE sliding window)
     * as a reusable, atomically-executed Redis/Valkey script.
     */
    @Bean
    public DefaultRedisScript<Long> rateLimiterScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/rate_limiter.lua"));
        script.setResultType(Long.class);
        return script;
    }
}
