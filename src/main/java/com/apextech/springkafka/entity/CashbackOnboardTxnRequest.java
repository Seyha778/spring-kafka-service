package com.apextech.springkafka.entity;

import com.apextech.springkafka.constant.TxnStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

@Entity
@Table(name = "cashback_onboard_txn_request", schema = "dgtl_backend")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CashbackOnboardTxnRequest {

    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "cotr_seq_gen")
    @SequenceGenerator(name = "cotr_seq_gen", sequenceName = "cashback_onboard_txn_request_seq", allocationSize = 1)
    private Long id;

    @Column(name = "run_id")
    private String runId;

    @Column(name = "customer_id")
    private String customerId;

    @Column(name = "cashback_amount")
    private BigDecimal cashbackAmount;

    @Column(name = "cashback_currency")
    private String cashbackCurrency;

    @Column(name = "customer_account")
    private String customerAccount;

    @Column(name = "funding_account")
    private String fundingAccount;

    @Column(name = "master_account_id")
    private String masterAccountId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private TxnStatus status;

    @Column(name = "picked_up_by")
    private String pickedUpBy;

    @Column(name = "picked_up_date")
    private LocalDateTime pickedUpDate;

    @Column(name = "trace_id")
    private String traceId;

    @Column(name = "span_id")
    private String spanId;

    @Column(name = "credit_txn_ref")
    private String creditTxnRef;

    @Column(name = "response_code")
    private String responseCode;

    @Column(name = "response_message")
    private String responseMessage;

    @Column(name = "retry_count")
    private Integer retryCount;

    @CreationTimestamp
    @Column(name = "created_on")
    private OffsetDateTime createdOn;

    @Column(name = "created_by")
    private String createdBy;

    @UpdateTimestamp
    @Column(name = "modified_on")
    private OffsetDateTime modifiedOn;

    @Column(name = "modified_by")
    private String modifiedBy;
}
