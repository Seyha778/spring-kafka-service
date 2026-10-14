package com.apextech.springkafka.service.impl;

import com.apextech.springkafka.constant.Constants;
import com.apextech.springkafka.constant.TxnStatus;
import com.apextech.springkafka.dto.CashbackTxnEvent;
import com.apextech.springkafka.entity.CashbackOnboardTxnRequest;
import com.apextech.springkafka.repository.CashbackOnboardTxnRequestRepository;
import com.apextech.springkafka.service.CashbackOnboardProcessService;
import com.apextech.springkafka.service.CashbackOnboardTxnStatusUpdater;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class CashbackOnboardProcessServiceImpl implements CashbackOnboardProcessService {

    private final CashbackOnboardTxnRequestRepository repository;
    private final CashbackOnboardTxnStatusUpdater statusUpdater;
    private final Environment environment;

    /**
     * Guards against double-crediting on redelivery (e.g. after a rate-limit
     * pause/resume, or a consumer rebalance before the ack lands) without an
     * extra network call: once a row is CREDITED or FAILED, a redelivered
     * event for the same id is skipped here.
     */
    @Override
    public void process(CashbackTxnEvent event) {
        CashbackOnboardTxnRequest row = repository
                .findById(event.getId())
                .orElseThrow(() -> new IllegalStateException("No cashback row found for id=" + event.getId()));

        if (TxnStatus.CREDIT_REQUEST_SENT == row.getStatus()) {
            log.warn(
                    "id={} customerId={} is stuck in CREDIT_REQUEST_SENT - a prior Send Money call may have crashed mid-flight. NOT auto-retrying - needs manual reconciliation with Send Money.",
                    event.getId(),
                    event.getCustomerId());
            return;
        }

        if (TxnStatus.IN_PROGRESS != row.getStatus()) {
            log.info(
                    "Skipping id={} customerId={} - status is not IN_PROGRESS (actual={})",
                    event.getId(),
                    event.getCustomerId(),
                    row.getStatus());
            return; // "Exit without processing" -> consumer still ACKs the offset
        }

        // Only this one still needs its own explicit transaction: it must commit
        // before the risky network call happens, unlike the final save below
        // which only runs after the call already returned.
        boolean claimed = statusUpdater.markCreditRequested(row.getId());
        if (!claimed) {
            log.warn(
                    "Lost the race to process id={} customerId={} - another concurrent attempt already claimed it. NOT calling Send Money.",
                    event.getId(),
                    event.getCustomerId());
            return;
        }

        // business logic here
        String applicationName = environment.getProperty("spring.application.name");
        row.setModifiedBy(applicationName);
        row.setStatus(TxnStatus.SUCCESS);

//        if (Objects.nonNull(response) && response.isSuccess()) {
////            row.setStatus(TxnStatus.SUCCESS);
////            row.setCreditTxnRef(response.getTxnRef());
////            row.setFundingAccount(response.getPrefundAccount());
////            row.setResponseCode(response.getResponseCode());
////            row.setResponseMessage(response.getResponseMessage());
//        } else {
//            row.setStatus(TxnStatus.FAILED);
//            row.setRetryCount(Objects.isNull(row.getRetryCount()) ? 1 : row.getRetryCount() + 1);
////            row.setResponseCode(Objects.isNull(response) ? Constants.UNKNOWN : response.getResponseCode());
////            row.setResponseMessage(
////                    Objects.isNull(response) ? "No response from Send Money" : response.getResponseMessage());
//        }
        repository.save(row);
    }
}
