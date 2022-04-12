package com.meituan.ptubes.sdk.consumer;

import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.constants.ConsumeConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.meituan.ptubes.sdk.protocol.RdsPacket;

public class DispatchThread extends AbstractActor {

    // private static final String DISPATCH_THREAD_PREFIX = "ptubes-dispatch-";
    private static final String DISPATCH_THREAD_PREFIX = "rds-cdc-dispatch-";

    private String taskName;
    private long dispatchEventCount;

    private BlockingQueue<RdsPacket.RdsEnvelope> envelopeQueue;
    private WorkThreadCluster workThreadCluster;
    private DispatchThreadState dispatchThreadState;

    public DispatchThread(
        String taskName,
        WorkThreadCluster workThreadCluster
    ) {
        super(
            DISPATCH_THREAD_PREFIX + taskName,
            LoggerFactory.getLoggerByTask(
                DispatchThread.class,
                taskName
            )
        );
        this.taskName = taskName;
        this.envelopeQueue = new LinkedBlockingDeque<>(ConsumeConstants.DISPATCH_BLOCKING_QUEUE_SIZE);
        this.workThreadCluster = workThreadCluster;
        this.dispatchThreadState = new DispatchThreadState();
    }

    public void addEnvelope(RdsPacket.RdsEnvelope rdsEnvelope) {
        boolean isPut = false;

        while (!isPut && !isShutdownRequested()) {
            try {
                int fetchQueueSize = envelopeQueue.size();
                log.debug("[fetch][queue][size]" + fetchQueueSize);
                isPut = this.envelopeQueue.offer(
                    rdsEnvelope,
                    ConsumeConstants.DISPATCH_THREAD_OFFER_TIMEOUT_MS,
                    TimeUnit.MILLISECONDS
                );
            } catch (Throwable ex) {
                log.error("Failed to put message into dispatch thread queue, ex: " + ex);
            }
        }
    }

    @Override
    protected boolean executeAndChangeState(Object message) {
        boolean success = true;

        if (message instanceof DispatchThreadState) {
            if (this.status != Status.RUNNING) {
                log.warn("Actor not running: " + message.toString());
            } else {
                DispatchThreadState dispatchThreadState = (DispatchThreadState) message;
                if (log.isDebugEnabled()) {
                    log.debug(String.format(
                        "DispatchThread %s current state: %s.",
                        this.getName(),
                        dispatchThreadState.getStateId()
                    ));
                }

                switch (dispatchThreadState.getStateId()) {
                    case DISPATCH_EVENT:
                        doDispatchEvent(dispatchThreadState);
                        break;
                    default:
                        log.error("Unknown state in Fetch thread: " + dispatchThreadState.getStateId());
                        success = false;
                        break;
                }
            }
        } else {
            success = super.executeAndChangeState(message);
        }

        return success;
    }

    private void doDispatchEvent(DispatchThreadState dispatchThreadState) {
        List<RdsPacket.RdsEnvelope> envelopeList = new ArrayList<>(ConsumeConstants.DISPATCH_BLOCKING_QUEUE_SIZE);
        RdsPacket.RdsEnvelope rdsEnvelope = null;
        int envelopeNum = 0;

        try {
            envelopeNum = envelopeQueue.drainTo(envelopeList);

            if (envelopeNum == 0) {

                    rdsEnvelope = envelopeQueue.poll(
                        ConsumeConstants.DISPATCH_THREAD_POLL_TIMEOUT_MS,
                        TimeUnit.MILLISECONDS
                    );

                    if (rdsEnvelope != null) {
                        envelopeList.add(rdsEnvelope);
                        envelopeNum = envelopeList.size();
                    }
            }
        } catch (Throwable e) {
            log.error(
                "Dispatch event fail.",
                e
            );

            try {
                Thread.sleep(ConsumeConstants.DISPATCH_THREAD_POLL_TIMEOUT_MS);
            } catch (InterruptedException ex) {
                log.error(
                    "Dispatch thread sleep exception: ",
                    e
                );
            }
        }

        for (int i = 0; i < envelopeNum; i++) {
            rdsEnvelope = envelopeList.get(i);
            doDispatch(rdsEnvelope);
        }

        dispatchThreadState.setStateId(DispatchThreadState.StateId.DISPATCH_EVENT);
        this.enqueueMessage(dispatchThreadState);
    }

    private void doDispatch(RdsPacket.RdsEnvelope rdsEnvelope) {
        try {
            if (rdsEnvelope.getMessageType()
                .equals(RdsPacket.MessageType.PARTITIONEDMESSAGE)) {
                RdsPacket.RdsPartitionedMessage partitionedMessage = RdsPacket.RdsPartitionedMessage.parseFrom(rdsEnvelope.getMessage());
                doPartitionedMessageDispatch(partitionedMessage);
            }
            // PB data of type MessageType.MESSAGE needs to be processed separately
        } catch (Throwable ex) {
            String errorMessage = "Critical error. WorkThread parse pb message error, might result in losing data!!!";

            log.error(
                errorMessage,
                ex
            );
        }

        if (log.isDebugEnabled()) {
            log.debug("Dispatch event total count: " + dispatchEventCount);
        }
    }

    private void doPartitionedMessageDispatch(RdsPacket.RdsPartitionedMessage partitionedMessage) {

        Map<Integer, RdsPacket.RdsMessage> messageMap = partitionedMessage.getPartitionMessagesMap();
        for (Entry<Integer, RdsPacket.RdsMessage> messageEntry : messageMap.entrySet()) {
            workThreadCluster.dispatchMessage(
                messageEntry.getKey(),
                messageEntry.getValue()
            );
            dispatchEventCount += messageEntry.getValue()
                .getMessageCount();
        }
    }

    @Override
    public void doStart(LifecycleMessage lifecycleMessage) {
        log.info("Dispatch thread start.");
        if (dispatchThreadState.getStateId() != DispatchThreadState.StateId.START) {
            return;
        }

        super.doStart(lifecycleMessage);
        dispatchThreadState.setStateId(DispatchThreadState.StateId.DISPATCH_EVENT);
        enqueueMessage(dispatchThreadState);
    }

    @Override
    protected void onShutdown() {

    }

    public String getTaskName() {
        return this.taskName;
    }

    public WorkThreadCluster getWorkThreadCluster() {
        return workThreadCluster;
    }
}
