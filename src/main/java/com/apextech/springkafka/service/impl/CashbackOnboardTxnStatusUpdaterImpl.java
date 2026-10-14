package com.apextech.springkafka.service.impl;

import com.apextech.springkafka.config.properties.CampaignOnboardProperties;
import com.apextech.springkafka.constant.TxnStatus;
import com.apextech.springkafka.repository.CashbackOnboardTxnRequestRepository;
import com.apextech.springkafka.service.CashbackOnboardTxnStatusUpdater;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

@Service
@RequiredArgsConstructor
public class CashbackOnboardTxnStatusUpdaterImpl implements CashbackOnboardTxnStatusUpdater {

    private final CashbackOnboardTxnRequestRepository repository;
    private final CampaignOnboardProperties properties;

    @Override
    public int markInProgress(String runId, List<Long> ids) {
        String instanceId = properties.instanceId();
        LocalDateTime now = LocalDateTime.now(ZoneId.of(properties.schedulerZone()));
        return repository.markInProgress(ids, TxnStatus.IN_PROGRESS, runId, instanceId, now);
    }

    /**
     * @return true if this call won the claim (row transitioned IN_PROGRESS ->
     *         CREDIT_REQUEST_SENT), false if another concurrent attempt already
     *         claimed it first - the caller must NOT call Send Money if false.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean markCreditRequested(Long id) {
        int updated = repository.claimForCreditRequest(id);
        return updated == 1;
    }
}
