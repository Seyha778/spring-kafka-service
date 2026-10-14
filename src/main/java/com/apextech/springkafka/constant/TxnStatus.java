package com.apextech.springkafka.constant;

/**
 * Status lifecycle as driven by the sequence diagram:
 * PENDING -> IN_PROGRESS (claimed by scheduler) -> CREDITED | FAILED
 *
 * Note: the DDL comment documents PENDING -> PROCESSING -> SUCCESS/FAILED as the
 * intended lifecycle. This service uses the literal values called out in the
 * sequence diagram (IN_PROGRESS / CREDITED) since that's the authoritative flow
 * for this implementation. Align naming with the data team if the DDL comment
 * reflects a differing convention.
 */
public enum TxnStatus {
    PENDING,
    IN_PROGRESS,
    CREDIT_REQUEST_SENT,
    SUCCESS,
    FAILED
}
