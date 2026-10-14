package com.apextech.springkafka.service;

import com.apextech.springkafka.dto.CashbackTxnEvent;

public interface CashbackOnboardProcessService {
    void process(CashbackTxnEvent event);
}
