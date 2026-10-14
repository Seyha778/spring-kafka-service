package com.apextech.springkafka.config;

import com.apextech.springkafka.config.properties.CampaignOnboardProperties;
import com.apextech.springkafka.config.properties.RateLimiterProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@EnableConfigurationProperties({CampaignOnboardProperties.class, RateLimiterProperties.class
})
public class AppConfig {

    /**
     * Used to schedule the delayed resume() of a paused Kafka listener container
     * after a rate-limit rejection (see CashbackOnboardConsumer).
     */
    @Bean
    public ThreadPoolTaskScheduler rateLimiterTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("rl-resume-");
        scheduler.initialize();
        return scheduler;
    }
}
