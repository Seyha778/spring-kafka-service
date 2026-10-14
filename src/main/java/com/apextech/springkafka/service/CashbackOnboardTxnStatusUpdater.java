package com.apextech.springkafka.service;

import java.util.List;

public interface CashbackOnboardTxnStatusUpdater {
    int markInProgress(String runId, List<Long> ids);

    boolean markCreditRequested(Long id);
}
