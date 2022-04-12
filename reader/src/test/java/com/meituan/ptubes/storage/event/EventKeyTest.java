package com.meituan.ptubes.storage.event;

import com.meituan.ptubes.reader.storage.common.event.EventKey;
import java.util.HashSet;
import java.util.Set;
import com.meituan.ptubes.common.exception.UnsupportedKeyException;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;


public class EventKeyTest {

    @Test
    public void test() throws Exception {
        Long longKey1 = 10L;
        Long longKey2 = 10L;
        Assert.assertEquals(
            new EventKey(longKey1).getLogicalPartitionId(),
            new EventKey(longKey2).getLogicalPartitionId()
        );

        Integer intKey1 = 10;
        Integer intKey2 = 10;
        Assert.assertEquals(
            new EventKey(intKey1).getLogicalPartitionId(),
            new EventKey(intKey2).getLogicalPartitionId()
        );

        String strKey1 = "abcd";
        String strKey2 = "abcd";
        Assert.assertEquals(
            new EventKey(strKey1).getLogicalPartitionId(),
            new EventKey(strKey2).getLogicalPartitionId()
        );
        Assert.assertEquals(
            new EventKey(strKey1.getBytes()).getLogicalPartitionId(),
            new EventKey(strKey2.getBytes()).getLogicalPartitionId()
        );

        try {
            EventKey eventKey = new EventKey(0.1);
            Assert.assertTrue(false);
        } catch (UnsupportedKeyException e) {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testPartitionIdDistributed() throws Exception {
        int maxPartitionCount = 128;
        Set<Short> longPartitionIds = new HashSet<>();
        Set<Short> intPartitionIds = new HashSet<>();
        Set<Short> strPartitionIds = new HashSet<>();
        for (int i = 0; i < maxPartitionCount * 20; i++) {
            Long longKey = (long) i;
            Integer intKey = i;
            String strKey = getRandomString();
            longPartitionIds.add(new EventKey(longKey).getLogicalPartitionId());
            intPartitionIds.add(new EventKey(intKey).getLogicalPartitionId());
            strPartitionIds.add(new EventKey(strKey).getLogicalPartitionId());
        }
        for (short partitionCount = 2; partitionCount <= maxPartitionCount; partitionCount++) {
            Set<Short> longPartitionNums = new HashSet<>();
            Set<Short> intPartitionNums = new HashSet<>();
            Set<Short> strPartitionNums = new HashSet<>();
            for (Short partitionId : longPartitionIds) {
                longPartitionNums.add((short) (partitionId % partitionCount));
            }
            for (Short partitionId : intPartitionIds) {
                intPartitionNums.add((short) (partitionId % partitionCount));
            }
            for (Short partitionId : strPartitionIds) {
                strPartitionNums.add((short) (partitionId % partitionCount));
            }
            Assert.assertEquals(longPartitionNums.size(), partitionCount);
            Assert.assertEquals(intPartitionNums.size(), partitionCount);
            Assert.assertEquals(strPartitionNums.size(), partitionCount);
        }

    }

    @Test
    public void testNegativeHashcode() throws Exception {
        // The hashcode of EventKey is Integer.MIN_VALUE
        Long longKey3 = 2009225688893999137L;
        Assert.assertEquals(
            2,
            new EventKey(longKey3).getLogicalPartitionId()
        );

        for (long i = 0; i < 1000000; i++) {
            long key = RandomUtils.nextLong(0,Long.MAX_VALUE);
            Assert.assertTrue(new EventKey(key).getLogicalPartitionId() >= 0);
        }

        for (long i = 0; i < 1000000; i++) {
            long key = -RandomUtils.nextLong(0,Long.MAX_VALUE);
            Assert.assertTrue(new EventKey(key).getLogicalPartitionId() >= 0);
        }
    }

    private String getRandomString() {
		return new String(RandomUtils.nextBytes(RandomUtils.nextInt(2,200)));
    }
}
