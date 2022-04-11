package com.meituan.ptubes.sdk.checkpoint;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



public class TestRdsCdcMysqlCheckpoint {

    private static MysqlCheckpoint checkpoint1;
    private static MysqlCheckpoint checkpoint2;
    private static MysqlCheckpoint checkpoint3;

    @Before
    public void setup() {
        checkpoint1 = new MysqlCheckpoint();
        checkpoint1.setVersionTs(1);
        checkpoint1.setCheckpointMode(BuffaloCheckpoint.CheckpointMode.EARLIEST);
        checkpoint1.setTimestamp(1);
        checkpoint1.setServerId(1);
        checkpoint1.setBinlogFile(1);
        checkpoint1.setBinlogOffset(1);
        checkpoint1.setEventIndex(1L);

        checkpoint2 = new MysqlCheckpoint();
        checkpoint2.setVersionTs(2);
        checkpoint2.setCheckpointMode(BuffaloCheckpoint.CheckpointMode.LATEST);
        checkpoint2.setTimestamp(2);
        checkpoint2.setServerId(2);
        checkpoint2.setBinlogFile(2);
        checkpoint2.setBinlogOffset(2);
        checkpoint2.setEventIndex(2L);

        checkpoint3 = new MysqlCheckpoint();
        checkpoint3.setVersionTs(1);
        checkpoint3.setCheckpointMode(BuffaloCheckpoint.CheckpointMode.EARLIEST);
        checkpoint3.setTimestamp(1);
        checkpoint3.setServerId(1);
        checkpoint3.setBinlogFile(1);
        checkpoint3.setBinlogOffset(1);
        checkpoint3.setEventIndex(1L);
    }

    @Test
    public void testCompare() {
        Assert.assertTrue(checkpoint1.compareTo(null) < 0);
        Assert.assertTrue(checkpoint1.compareTo(checkpoint2) > 0);

        checkpoint2.setVersionTs(1);
        Assert.assertTrue(checkpoint1.compareTo(checkpoint2) == 0);

        checkpoint1.setCheckpointMode(BuffaloCheckpoint.CheckpointMode.NORMAL);
        Assert.assertTrue(checkpoint1.compareTo(checkpoint2) < 0);

        checkpoint2.setCheckpointMode(BuffaloCheckpoint.CheckpointMode.NORMAL);
        Assert.assertTrue(checkpoint1.compareTo(checkpoint2) < 0);

        checkpoint2.setTimestamp(1);
        Assert.assertTrue(checkpoint1.compareTo(checkpoint2) == 0);

        checkpoint2.setServerId(1);
        Assert.assertTrue(checkpoint1.compareTo(checkpoint2) < 0);

        checkpoint2.setBinlogFile(1);
        Assert.assertTrue(checkpoint1.compareTo(checkpoint2) < 0);

        checkpoint2.setBinlogOffset(1);
        Assert.assertTrue(checkpoint1.compareTo(checkpoint2) < 0);

        checkpoint2.setEventIndex(1);
        Assert.assertTrue(checkpoint1.compareTo(checkpoint2) == 0);
    }


    @Test
    public void testEquals() {
        Assert.assertTrue(checkpoint1.equals(checkpoint3));
        Assert.assertFalse(checkpoint1.equals(checkpoint2));
    }
}
