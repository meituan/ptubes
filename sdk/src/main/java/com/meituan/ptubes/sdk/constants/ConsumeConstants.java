package com.meituan.ptubes.sdk.constants;



public class ConsumeConstants {

    public static final long FETCH_PICKREADER_INTERVAL_SEC = 10;
    public static final long FETCH_REBALANCE_INTERVAL_MS = 90 * 1000;
    public static final int FETCH_REBALANCE_THRESHOLD = 20;
    public static final int FETCH_REQUEST_TIMEOUT_MS = 2000;
    public static final int FETCH_LOG_INTERVAL = 30;
    public static final long FETCH_CONNECT_EERROR_INTERVAL = 2000;
    public static final long FETCH_SUBSCRIBE_EERROR_INTERVAL = 2000;

    public static final int DISPATCH_THREAD_POLL_TIMEOUT_MS = 500;
    public static final int DISPATCH_THREAD_OFFER_TIMEOUT_MS = 500;
    public static final int DISPATCH_BLOCKING_QUEUE_SIZE = 15;

    public static final int WORKTHREAD_POLL_TIMEOUT_MS = 50;
    public static final int WORKTHREAD_OFFER_TIMEOUT_MS = 500;
    public static final int WORKTHREAD_BLOCKING_QUEUE_SIZE = 100;
    public static final int WORKTHREAD_RETRY_SLEEP_INTERVAL = 3;
    public static final int WORKTHREAD_RETRY_SLEEP_MS = 100;
    public static final int HELIX_PROPERTY_STORE_RETRY_INTERVAL_MS = 500;
}
