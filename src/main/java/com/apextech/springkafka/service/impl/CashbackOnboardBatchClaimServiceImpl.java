package com.apextech.springkafka.service.impl;

import com.apextech.springkafka.config.properties.CampaignOnboardProperties;
import com.apextech.springkafka.dto.CashbackTxnProjection;
import com.apextech.springkafka.kafka.CashbackOnboardProducer;
import com.apextech.springkafka.repository.CashbackOnboardTxnRequestRepository;
import com.apextech.springkafka.service.CashbackOnboardBatchClaimService;
import com.apextech.springkafka.service.CashbackOnboardTxnStatusUpdater;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class CashbackOnboardBatchClaimServiceImpl implements CashbackOnboardBatchClaimService {

    private final CashbackOnboardTxnRequestRepository repository;
    private final CashbackOnboardTxnStatusUpdater statusUpdater;
    private final CashbackOnboardProducer producer;
    private final CampaignOnboardProperties properties;

    @Override
    @Transactional
    public int claimAndPublishNextBatch(String runId) {
        ZoneId zone = ZoneId.of(properties.schedulerZone());

        // Start of day is set to minus 1 day because the data is one day behind.
        OffsetDateTime startOfDay =
                LocalDate.now(zone).minusDays(1).atStartOfDay(zone).toOffsetDateTime();

        // End of day is set to +1 day (24 hours) from the start of day.
        OffsetDateTime endOfDay = startOfDay.plusDays(1);

        log.info("Claimed Next Batch | startOfDay={} | endOfDay={}", startOfDay, endOfDay);
        List<CashbackTxnProjection> claimed =
                repository.findNextPendingBatch(startOfDay, endOfDay, PageRequest.of(0, properties.batchSize()));

        if (claimed.isEmpty()) {
            log.debug("No PENDING rows to claim. runId={}", runId);
            return 0;
        }

        List<Long> ids = claimed.stream().map(CashbackTxnProjection::id).toList();

        int updated = statusUpdater.markInProgress(runId, ids);
        if (updated != claimed.size()) {
            log.warn(
                    "markInProgress affected {} rows but {} were claimed. runId={} ids={}",
                    updated,
                    claimed.size(),
                    runId,
                    ids);
        }

        producer.publishBatch(runId, claimed);

        log.info("Claimed and published batch. runId={} size={}", runId, claimed.size());
        return claimed.size();
    }
}
