package com.meituan.ptubes.sdk;



public enum AckStatus {
    OK, // success
    FAIL, // normal failure, need to retry
    FAIL_WITH_EXCEPTION, // Unknown failure, need to retry, may need manual intervention
    FAIL_NOT_EXISTENT_PARTITION, // The shard does not exist (for example, it has been migrated)
    FAIL_WRONG_ACK_ID, // ackId format is wrong
    FAIL_CONNECTOR_IS_NOT_RUNNING, // connector has been closed
    FAIL_NO_NEED_FOR_ACK, // not an asynchronous task
    UNKNOWN; // Unknown failure (there is no such scenario yet, keep the extension)
}
