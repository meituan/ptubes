package com.meituan.ptubes.sdk.consumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



public class TestDelayStrategy {

    private DelayStrategy delayStrategy;
    private int initSleep;
    private int sleepIncFactor;
    private int sleepIncDelta;
    private int maxSleep;

    @Before
    public void setup() {
        initSleep = 0;
        sleepIncFactor = 2;
        sleepIncDelta = 1;
        maxSleep = 1000;
    }

    @Test
    public void testLinearDelayStrategy() {
        delayStrategy = new LinearDelayStrategy(
            initSleep,
            sleepIncFactor,
            sleepIncDelta,
            maxSleep
        );

        int expectSleepTime = initSleep;
        int expectSleepCount = 0;

        for (int i = 0; i < 10; i++) {
            delayStrategy.sleep();

            int nextSleepTime = sleepIncFactor * expectSleepTime + sleepIncDelta;
            expectSleepTime = Math.min(
                nextSleepTime,
                maxSleep
            );
            expectSleepCount++;

            Assert.assertEquals(delayStrategy.getSleepTime(), expectSleepTime);
            Assert.assertEquals(delayStrategy.getSleepCount(), expectSleepCount);
        }

        delayStrategy.reset();
        Assert.assertEquals(delayStrategy.getSleepTime(), initSleep);
        Assert.assertEquals(delayStrategy.getSleepCount(), 0);
    }
}
