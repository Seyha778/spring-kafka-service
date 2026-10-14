package com.apextech.springkafka.service;

public interface CashbackOnboardNotifyService {

    void recordAttemptAndNotifyIfComplete(String runId);
}
