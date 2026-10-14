package com.apextech.springkafka.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.campaign.onboard")
public record CampaignOnboardProperties(
        String topic, String groupId, int batchSize, int maxBatchesPerRun, String schedulerZone, String instanceId) {}
