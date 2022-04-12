package com.meituan.ptubes.sdk.consumer;



public interface DelayStrategy {

    boolean sleep();

    void reset();

    int getSleepTime();

    int getSleepCount();
}
