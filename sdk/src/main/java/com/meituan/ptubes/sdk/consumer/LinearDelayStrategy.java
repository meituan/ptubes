package com.meituan.ptubes.sdk.consumer;

public class LinearDelayStrategy implements DelayStrategy {

    private final int initSleep;
    private final int sleepIncFactor;
    private final int sleepIncDelta;
    private final int maxSleep;

    private int sleepTime;
    private int sleepCount;

    public LinearDelayStrategy(
        int initSleep,
        int sleepIncFactor,
        int sleepIncDelta,
        int maxSleep
    ) {
        this.initSleep = initSleep;
        this.sleepIncFactor = sleepIncFactor;
        this.sleepIncDelta = sleepIncDelta;
        this.maxSleep = maxSleep;

        this.sleepTime = this.initSleep;
        this.sleepCount = 0;
    }

    @Override
    public boolean sleep() {
        int nextSleepTime = this.sleepIncFactor * this.sleepTime + this.sleepIncDelta;
        this.sleepTime = Math.min(
            nextSleepTime,
            this.maxSleep
        );
        this.sleepCount++;

        try {
            Thread.sleep(this.sleepTime);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public void reset() {
        this.sleepTime = this.initSleep;
        this.sleepCount = 0;
    }

    @Override
    public int getSleepTime() {
        return this.sleepTime;
    }

    @Override
    public int getSleepCount() {
        return this.sleepCount;
    }
}
