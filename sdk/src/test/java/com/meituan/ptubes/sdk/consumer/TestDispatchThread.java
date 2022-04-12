package com.meituan.ptubes.sdk.consumer;

import com.google.protobuf.ByteString;
import com.meituan.ptubes.sdk.checkpoint.CheckpointPersistenceProviderFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.meituan.ptubes.sdk.config.PtubesSdkConsumerConfig;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;
import com.meituan.ptubes.sdk.config.WorkThreadConfig;
import com.meituan.ptubes.sdk.protocol.RdsPacket;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDispatchThread {

    private int eventConsume;
    private int partitionId;
    private String taskName;

    private WorkThreadConfig workThreadConfig;
    private WorkThreadCluster workThreadCluster;
    private DispatchThread dispatchThread;
    private RdsPacket.RdsEnvelope rdsEnvelope;
    private TestWorkThread.TestRdsCdcEventListener eventListener;

    @Before
    public void setup() {
        taskName = "testTask";
        partitionId = 0;

        eventConsume = 100000;
        workThreadConfig = new WorkThreadConfig();
        workThreadConfig.setTaskName("testTask");
        workThreadConfig.setConsumptionMode(PtubesSdkConsumerConfig.ConsumptionMode.SINGLE);
        workThreadConfig.setCheckpointSyncIntervalMs(120000);
        workThreadConfig.setRetryTimes(-1);
        workThreadConfig.setFailureMode(PtubesSdkConsumerConfig.FailureMode.RETRY);
        workThreadConfig.setSourceType(RdsCdcSourceType.MYSQL);

        eventListener = new TestWorkThread.TestRdsCdcEventListener();

        workThreadCluster = new WorkThreadCluster(
            taskName,
            workThreadConfig,
            eventListener
        );

        dispatchThread = new DispatchThread(
            taskName,
            workThreadCluster
        );
        dispatchThread.enqueueMessage(LifecycleMessage.createStartMessage());

        UncaughtExceptionTrackingThread dispatchWrappedThread = new UncaughtExceptionTrackingThread(
            dispatchThread,
            dispatchThread.getName(), 
            dispatchThread.getTaskName()
        );

        CheckpointPersistenceProviderFactory.initPersistenceProvider(
            taskName,
            new TestWorkThread.TestCheckpointProvider()
        );

        workThreadCluster.start();

        workThreadCluster.addWorkThread(partitionId);

        dispatchWrappedThread.setDaemon(true);
        dispatchWrappedThread.start();

        workThreadCluster.startPartition(partitionId);

        rdsEnvelope = buildRdsTestEnvelope(
            eventConsume,
            partitionId
        );
        dispatchThread.addEnvelope(rdsEnvelope);
    }

    RdsPacket.RdsEnvelope buildRdsTestEnvelope(
        int eventSize,
        int partitionId
    ) {
        List<ByteString> rdsEventList = new ArrayList<>();

        for (int i = 0; i < eventSize; i++) {
            rdsEventList.add(RdsPacket.RdsEvent.newBuilder()
                                     .build()
                                     .toByteString());
        }

        RdsPacket.RdsMessage rdsMessage = RdsPacket.RdsMessage.newBuilder()
            .addAllMessages(rdsEventList)
            .build();
        RdsPacket.RdsPartitionedMessage rdsPartitionedMessage = RdsPacket.RdsPartitionedMessage.newBuilder()
            .putPartitionMessages(
                partitionId,
                rdsMessage
            )
            .build();

        return RdsPacket.RdsEnvelope.newBuilder()
            .setMessageType(RdsPacket.MessageType.PARTITIONEDMESSAGE)
            .setMessage(rdsPartitionedMessage.toByteString())
            .build();
    }

    @Test
    public void testEventCome() {
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (Exception e) {

        }

        Assert.assertEquals(
            eventConsume,
            eventListener.eventCount
        );
    }
}
