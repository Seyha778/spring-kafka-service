package com.apextech.springkafka.service;

public interface CashbackOnboardBatchClaimService {
    int claimAndPublishNextBatch(String runId);
}
