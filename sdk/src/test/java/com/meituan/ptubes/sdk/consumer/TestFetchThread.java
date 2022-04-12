package com.meituan.ptubes.sdk.consumer;

import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.sdk.config.RdsCdcClientConfigManager;
import com.meituan.ptubes.sdk.config.notification.SimpleLocalFileConfigChangeNotifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



public class TestFetchThread {

    private String taskName;
    private FetchThread fetchThread;

    @Before
    public void setup() {
        taskName = "clientDemo";

        RdsCdcClientConfigManager rdsCdcClientConfigManager =
                new RdsCdcClientConfigManager(taskName, new SimpleLocalFileConfigChangeNotifier(taskName));
        rdsCdcClientConfigManager.init();

        fetchThread = new FetchThread(
            taskName,
            null,
            rdsCdcClientConfigManager.getFetchThreadConfig()
        );

    }

    @Test
    public void testUpdateCurrentCheckpoint() {
        long nowTime = System.currentTimeMillis() + 1;

        MysqlCheckpoint rdsCdcMysqlCheckpoint = new MysqlCheckpoint();
        rdsCdcMysqlCheckpoint.setCheckpointMode(BuffaloCheckpoint.CheckpointMode.NORMAL);
        rdsCdcMysqlCheckpoint.setTimestamp(nowTime);
        rdsCdcMysqlCheckpoint.setVersionTs(nowTime);

        MysqlCheckpoint rdsCdcMysqlCheckpoint2 = new MysqlCheckpoint();
        rdsCdcMysqlCheckpoint2.setCheckpointMode(BuffaloCheckpoint.CheckpointMode.NORMAL);
        rdsCdcMysqlCheckpoint2.setTimestamp(nowTime);
        rdsCdcMysqlCheckpoint2.setVersionTs(nowTime - 1000);

        fetchThread.updateCurrentCheckpoint(
                rdsCdcMysqlCheckpoint,
            true,
            false
        );

        Assert.assertEquals(
                rdsCdcMysqlCheckpoint,
            fetchThread.getCurrentCheckpoint()
        );

        fetchThread.updateCurrentCheckpoint(
                rdsCdcMysqlCheckpoint2,
            true,
            false
        );

        Assert.assertEquals(
                rdsCdcMysqlCheckpoint,
            fetchThread.getCurrentCheckpoint()
        );

        fetchThread.updateCurrentCheckpoint(
                rdsCdcMysqlCheckpoint2,
            true,
            true
        );

        Assert.assertEquals(
                rdsCdcMysqlCheckpoint2,
            fetchThread.getCurrentCheckpoint()
        );
    }
}
