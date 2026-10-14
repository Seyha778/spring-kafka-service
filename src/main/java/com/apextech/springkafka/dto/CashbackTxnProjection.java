package com.apextech.springkafka.dto;

import java.math.BigDecimal;

public record CashbackTxnProjection(
        Long id,
        BigDecimal cashbackAmount,
        String cashbackCurrency,
        String customerAccount,
        String fundingAccount,
        String masterAccountId,
        String customerId) {}
