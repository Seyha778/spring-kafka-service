package com.apextech.springkafka.kafka;

import com.apextech.springkafka.config.properties.CampaignOnboardProperties;
import com.apextech.springkafka.dto.CashbackTxnEvent;
import com.apextech.springkafka.dto.CashbackTxnProjection;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Component
@RequiredArgsConstructor
@Slf4j
public class CashbackOnboardProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final CampaignOnboardProperties campaignOnboardProperties;

    /**
     * Publishes one record per claimed row. Producer-side batching (linger.ms/batch.size,
     * see KafkaProducerConfig) coalesces these into fewer network requests -
     * no manual batching is needed here.
     */
    public void publishBatch(String runId, List<CashbackTxnProjection> rows) {
        for (CashbackTxnProjection row : rows) {
            CashbackTxnEvent event = CashbackTxnEvent.builder()
                    .id(row.id())
                    .runId(runId)
                    .customerId(row.customerId())
                    .cashbackAmount(row.cashbackAmount())
                    .cashbackCurrency(row.cashbackCurrency())
                    .customerAccount(row.customerAccount())
                    .fundingAccount(row.fundingAccount())
                    .masterAccountId(row.masterAccountId())
                    .build();

            // key by customerId so all events for the same customer land on the same
            // partition, preserving per-customer ordering
            kafkaTemplate
                    .send(campaignOnboardProperties.topic(), row.customerId(), event)
                    .whenComplete((result, ex) -> {
                        if (Objects.nonNull(ex)) {
                            log.error(
                                    "Failed to publish cashback event id={} customerId={}",
                                    row.id(),
                                    row.customerId(),
                                    ex);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Published cashback event id={} partition={} offset={}",
                                        row.id(),
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                            }
                        }
                    });
        }
    }
}
