package com.apextech.springkafka.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CashbackTxnEvent {
    private Long id;
    private String runId;
    private String customerId;
    private BigDecimal cashbackAmount;
    private String cashbackCurrency;
    private String customerAccount;
    private String fundingAccount;
    private String masterAccountId;
    private String serviceType;
    private String serviceSubType;
    private String source;
    private String traceId;
}
