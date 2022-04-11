package com.meituan.ptubes.sdk.consumer;

import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.ICheckpointPersistenceProvider;
import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.PartitionStorage;
import com.meituan.ptubes.sdk.constants.MonitorConstants;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import com.meituan.ptubes.sdk.IRdsCdcEventListener;
import com.meituan.ptubes.sdk.RdsCdcEventStatus;
import com.meituan.ptubes.sdk.config.PtubesSdkConsumerConfig;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;
import com.meituan.ptubes.sdk.config.WorkThreadConfig;
import com.meituan.ptubes.sdk.model.PartitionClusterInfo;
import com.meituan.ptubes.sdk.protocol.RdsPacket.Column;
import com.meituan.ptubes.sdk.protocol.RdsPacket.EventType;
import com.meituan.ptubes.sdk.protocol.RdsPacket.RdsEvent;
import com.meituan.ptubes.sdk.protocol.RdsPacket.RowData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



public class TestWorkThread {

    private int eventCount;

    private WorkThreadConfig workThreadConfig;
    private WorkThread workThread;

    private TestRdsCdcEventListener eventListener;

    @Before
    public void setup() {
        eventCount = 100000;

        eventListener = new TestRdsCdcEventListener();

        workThreadConfig = new WorkThreadConfig();
        workThreadConfig.setTaskName("testTask");
        workThreadConfig.setConsumptionMode(PtubesSdkConsumerConfig.ConsumptionMode.SINGLE);
        workThreadConfig.setCheckpointSyncIntervalMs(120000);
        workThreadConfig.setRetryTimes(-1);
        workThreadConfig.setFailureMode(PtubesSdkConsumerConfig.FailureMode.RETRY);
        workThreadConfig.setSourceType(RdsCdcSourceType.MYSQL);

        MysqlCheckpoint rdsCdcCheckpoint = new MysqlCheckpoint();
        rdsCdcCheckpoint.setCheckpointMode(BuffaloCheckpoint.CheckpointMode.EARLIEST);

        workThread = new WorkThread(
                1,
                "testThread",
                workThreadConfig,
                eventListener,
                new TestCheckpointProvider(),
                new PartitionClusterInfo()
        );

        workThread.enqueueMessage(LifecycleMessage.createStartMessage());

        UncaughtExceptionTrackingThread wrappedWorkThread = new UncaughtExceptionTrackingThread(workThread, workThreadConfig.getTaskName());
        wrappedWorkThread.setDaemon(true);
        wrappedWorkThread.start();
    }

    @Test(timeout = 10000L)
    public void testEventCome() {
        for (int i = 0; i < eventCount; i++) {
            RdsEvent rdsEvent = RdsEvent.newBuilder()
                .build();
            workThread.addEventString(rdsEvent.toByteString());
        }

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception e) {

        }

        Assert.assertEquals(
            eventListener.eventCount,
            eventCount
        );

    }

    @Test(timeout = 10000L)
    public void testBatchEventCome() {
        workThreadConfig.setConsumptionMode(PtubesSdkConsumerConfig.ConsumptionMode.BATCH);
        workThreadConfig.setBatchSize(20);
        workThreadConfig.setBatchTimeoutMs(100);

        for (int i = 0; i < eventCount; i++) {
            RdsEvent rdsEvent = RdsEvent.newBuilder()
                .build();
            workThread.addEventString(rdsEvent.toByteString());
        }

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception e) {

        }

        Assert.assertEquals(
            eventListener.eventCount,
            eventCount
        );
    }

    @Test
    public void testHeartbeat() {
        long timestamp = System.currentTimeMillis();
        Column utime = Column.newBuilder()
            .setName(MonitorConstants.HEARTBEAT_COLUMN_NAME)
            .setValue(Long.toString(timestamp))
            .build();
        RowData rowData = RowData.newBuilder()
            .putAfterColumns(
                MonitorConstants.HEARTBEAT_COLUMN_NAME,
                utime
            )
            .build();

        RdsEvent rdsEvent = RdsEvent.newBuilder()
            .setEventType(EventType.HEARTBEAT)
            .setRowData(rowData)
            .build();
        workThread.addEventString(rdsEvent.toByteString());
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception e) {

        }

        Assert.assertEquals(
            timestamp,
            workThread.getWorkMonitorInfo()
                .getHeartbeatTimestamp()
        );
    }

    public static class TestRdsCdcEventListener implements IRdsCdcEventListener {

        volatile int eventCount = 0;

        @Override
        public RdsCdcEventStatus onEvents(List<RdsEvent> events) {
            if (new Random().nextInt() % 10000 == 0) {
                throw new RuntimeException("Random throw exception.");
            }
            if (new Random().nextInt() % 10000 == 0) {
                return RdsCdcEventStatus.FAILURE;
            }

            eventCount += events.size();
            return RdsCdcEventStatus.SUCCESS;
        }
    }

    public static class TestCheckpointProvider implements ICheckpointPersistenceProvider {


        @Override
        public void storeCheckpointIfNotExists(int partitionId, BuffaloCheckpoint checkpoint) {

        }

        @Override
        public void storeCheckpointIfExists(int partitionId, BuffaloCheckpoint checkpoint) {

        }


        @Override
        public MysqlCheckpoint loadCheckpoint(int partitionId) {
            return null;
        }

        @Override
        public void deleteCheckpoint(int partitionId) {

        }

        @Override
        public void deleteCheckpoints(List<Integer> partitionIds) {

        }

        @Override
        public void storePartitionStorage(PartitionStorage partitionStorage) {

        }

        @Override
        public PartitionStorage loadPartitionStorage() {
            return null;
        }

        @Override
        public MysqlCheckpoint getEarliestCheckpoint(int totalPartition) {
            return null;
        }

        @Override
        public PartitionStorage buildPartitionStorage(RdsCdcSourceType sourceType, int partitionNum, BuffaloCheckpoint checkpoint) {
            return null;
        }

        @Override
        public void close() {

        }
    }
}
