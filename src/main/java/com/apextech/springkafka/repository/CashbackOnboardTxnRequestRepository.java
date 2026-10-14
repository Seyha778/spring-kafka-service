package com.apextech.springkafka.repository;

import com.apextech.springkafka.constant.TxnStatus;
import com.apextech.springkafka.dto.CashbackTxnProjection;
import com.apextech.springkafka.entity.CashbackOnboardTxnRequest;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;

public interface CashbackOnboardTxnRequestRepository extends JpaRepository<CashbackOnboardTxnRequest, Long> {

    /**
     * Claims up to {@code pageable.getPageSize()} PENDING rows within the given
     * time range. Uses {@link Pageable} for limiting and {@code SKIP LOCKED}
     * semantics to avoid concurrent processing of the same rows.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query(
            """
            SELECT new com.apextech.springkafka.dto.CashbackTxnProjection(
                        t.id,t.cashbackAmount,t.cashbackCurrency,t.customerAccount,t.fundingAccount,t.masterAccountId,t.customerId)
             FROM CashbackOnboardTxnRequest t
             WHERE t.createdOn >= :startOfDay
               AND t.createdOn < :endOfDay
               AND t.status = com.apextech.springkafka.constant.TxnStatus.PENDING
             ORDER BY t.id
            """)
    List<CashbackTxnProjection> findNextPendingBatch(
            @Param("startOfDay") OffsetDateTime startOfDay,
            @Param("endOfDay") OffsetDateTime endOfDay,
            Pageable pageable);

    @Modifying
    @Transactional
    @Query(
            """
            UPDATE CashbackOnboardTxnRequest t
               SET t.status = :status,
                   t.runId = :runId,
                   t.pickedUpBy = :pickedUpBy,
                   t.pickedUpDate = :pickedUpDate,
                   t.modifiedBy = :pickedUpBy
             WHERE t.id IN :ids
            """)
    int markInProgress(
            @Param("ids") List<Long> ids,
            @Param("status") TxnStatus status,
            @Param("runId") String runId,
            @Param("pickedUpBy") String pickedUpBy,
            @Param("pickedUpDate") LocalDateTime pickedUpDate);

    @Modifying
    @Transactional
    @Query(
            """
            UPDATE CashbackOnboardTxnRequest t
               SET t.status = com.apextech.springkafka.constant.TxnStatus.CREDIT_REQUEST_SENT
             WHERE t.id = :id
               AND t.status = com.apextech.springkafka.constant.TxnStatus.IN_PROGRESS
            """)
    int claimForCreditRequest(@Param("id") Long id);

    List<CashbackOnboardTxnRequest> findAllByRunId(String runId);
}
