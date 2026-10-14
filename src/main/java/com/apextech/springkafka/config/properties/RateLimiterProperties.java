package com.apextech.springkafka.config.properties;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.rate-limiter")
public record RateLimiterProperties(
        @NotBlank String keyPrefix,
        @Positive int limitPerWindow,
        @Positive long windowMillis,
        @Min(1) long retryBackoffMillis) {}
