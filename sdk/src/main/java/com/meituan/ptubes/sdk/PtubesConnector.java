package com.meituan.ptubes.sdk;

import com.meituan.ptubes.common.exception.RdsCdcRuntimeException;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.RdsCdcCheckpointFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterManager;
import com.meituan.ptubes.sdk.cluster.zk.ZKRdsCdcClusterManager;
import com.meituan.ptubes.sdk.config.RdsCdcClientConfigManager;
import com.meituan.ptubes.sdk.config.notification.IConfigChangeNotifier;
import com.meituan.ptubes.sdk.consumer.DispatchThread;
import com.meituan.ptubes.sdk.consumer.FetchThread;
import com.meituan.ptubes.sdk.consumer.LifecycleMessage;
import com.meituan.ptubes.sdk.consumer.UncaughtExceptionTrackingThread;
import com.meituan.ptubes.sdk.consumer.WorkThread;
import com.meituan.ptubes.sdk.consumer.WorkThreadCluster;
import com.meituan.ptubes.sdk.monitor.ConnectorMonitorInfo;
import com.meituan.ptubes.sdk.monitor.WorkMonitorInfo;

public class PtubesConnector<C extends BuffaloCheckpoint> implements IPtubesConnector<C> {

    private final Logger log;
    private final String taskName;

    private RdsCdcClientConfigManager rdsCdcClientConfigManager;
    private FetchThread fetchThread;
    private UncaughtExceptionTrackingThread fetchWrappedThread;
    private DispatchThread dispatchThread;
    private UncaughtExceptionTrackingThread dispatchWrappedThread;
    private WorkThreadCluster workThreadCluster;
    private IRdsCdcEventListener rdsCdcEventListener;

    private IRdsCdcClusterManager clusterManager;

    private ConnectorStatus connectorStatus;

    private final ReentrantLock controlLock = new ReentrantLock(true);
    private final Condition shutdownCondition = controlLock.newCondition();
    private volatile boolean showdownRequested = false;

    private final long startTs;

    PtubesConnector(
            String taskName,
            RdsCdcClientConfigManager rdsCdcClientConfigManager
    ) {
        this.log = LoggerFactory.getLoggerByTask(
                PtubesConnector.class,
                taskName
        );

        this.taskName = taskName;
        this.rdsCdcClientConfigManager = rdsCdcClientConfigManager;
        this.connectorStatus = ConnectorStatus.INIT;
        this.startTs = System.currentTimeMillis();
    }

    public static PtubesConnector<MysqlCheckpoint> newMySQLConnection(String taskName, IConfigChangeNotifier iConfigChangeNotifier, IRdsCdcEventListener iRdsCdcEventListener) throws Exception {
        return ((PtubesConnector<MysqlCheckpoint>) RdsCdcConnectorFactory.buildMySQLConnector(taskName, iConfigChangeNotifier, iRdsCdcEventListener));
    }

    @Override
    public void registerEventListener(IRdsCdcEventListener eventListener)
            throws RdsCdcRuntimeException {
        if (this.rdsCdcEventListener != null) {
            throw new RdsCdcRuntimeException("RdsCdcEventListener is not null, cannot be registered again.");
        }

        this.rdsCdcEventListener = eventListener;
    }

    @Override
    public void startup() throws Exception {
        log.info("initialize a new rds cdc client [" + this.taskName + "].");
        try {
            this.workThreadCluster = new WorkThreadCluster(
                    taskName,
                    rdsCdcClientConfigManager.getWorkThreadConfig(),
                    rdsCdcEventListener
            );

            this.dispatchThread = new DispatchThread(
                    taskName,
                    workThreadCluster
            );

            this.fetchThread = new FetchThread(
                    taskName,
                    dispatchThread,
                    rdsCdcClientConfigManager.getFetchThreadConfig()
            );

            this.fetchThread.enqueueMessage(LifecycleMessage.createStartMessage());
            this.dispatchThread.enqueueMessage(LifecycleMessage.createStartMessage());

            this.fetchWrappedThread = new UncaughtExceptionTrackingThread(
                    fetchThread,
                    fetchThread.getName(),
                    taskName
            );

            this.dispatchWrappedThread = new UncaughtExceptionTrackingThread(
                    dispatchThread,
                    dispatchThread.getName(),
                    taskName
            );

            this.clusterManager = new ZKRdsCdcClusterManager(
                    rdsCdcClientConfigManager.getClusterConfig(),
                    fetchThread,
                    workThreadCluster
            );

            // workThreadCluster needs to be started before the clusterManager starts, to avoid the helix callback of adding or removing shards before the workThreadCluster is started
            this.workThreadCluster.setReadOnlyPartitionClusterInfo(
                    rdsCdcClientConfigManager.getFetchThreadConfig().getReaderConnectionConfig()
                            .getPartitionClusterInfo()); // This field is shared with fetchThread to wrap the total number of partitions into the callback structure when the asynchronous callback has user functions
            this.workThreadCluster.start();
            this.clusterManager.start();

            this.dispatchWrappedThread.setDaemon(true);
            this.dispatchWrappedThread.start();

            this.fetchWrappedThread.setDaemon(true);
            this.fetchWrappedThread.start();

            this.rdsCdcClientConfigManager.getDefaultConfChangedRecipient().init(workThreadCluster, fetchThread, clusterManager);
            this.rdsCdcClientConfigManager.getConfigChangeNotifier().registerAllListener();

            this.connectorStatus = ConnectorStatus.RUNNING;
        } catch (Throwable t) {
            log.error(
                    // "fail to initilize ptubes client " + this.taskName,
                    "fail to initilize rds cdc client " + this.taskName,
                    t
            );
            throw t;
        }

        log.info(String.format(
                // "A new ptubes client (%s) successfully initialized.",
                "A new rds cdc client (%s) successfully initialized.",
                this.taskName
        ));
        this.awaitShutdown();
        log.info("[connectorQuit][" + this.taskName + "]");
    }

    /**
     * Temporarily unavailable
     * @throws Exception
     */
    @Override
    public void suspend() throws Exception {
        log.info("try to suspend a rds cdc client [" + this.taskName + "].");

        try {
            this.fetchThread.pause();
            this.dispatchThread.pause();
            this.workThreadCluster.pause();

            this.connectorStatus = ConnectorStatus.SUSPENDED;
        } catch (Throwable t) {
            log.error(
                    // "fail to suspend ptubes client " + this.taskName,
                    "fail to suspend rds cdc client " + this.taskName,
                    t
            );
            throw t;
        }

        log.info(String.format(
                // "A ptubes client (%s) successfully suspended.",
                "A rds cdc client (%s) successfully suspended.",
                this.taskName
        ));
    }

    /**
     * Temporarily unavailable
     * @throws Exception
     */
    @Deprecated
    @Override
    public void resume() throws Exception {
        log.info("try to resume a rds cdc client [" + this.taskName + "].");

        try {
            this.fetchThread.resume();
            this.dispatchThread.resume();
            this.workThreadCluster.resume();

            this.connectorStatus = ConnectorStatus.RUNNING;
        } catch (Throwable t) {
            log.error(
                    // "fail to resume ptubes client " + this.taskName,
                    "fail to resume rds cdc client " + this.taskName,
                    t
            );
            throw t;
        }

        log.info(String.format(
                // "A ptubes client (%s) successfully resumed.",
                "A rds cdc client (%s) successfully resumed.",
                this.taskName
        ));
    }

    @Override
    public void shutdown() {
        log.info("[shutConnect]try to shutdown a rds cdc client [" + this.taskName + "].");

        try {
            controlLock.lock();

            try {
                if (!showdownRequested) {
                    log.info("[shutConnect][" + this.taskName + "] " + "shutdown lion listener.");
                    this.rdsCdcClientConfigManager.getConfigChangeNotifier().deRegisterAllListener();
                    if (clusterManager != null) {
                        this.clusterManager.shutdown();
                    }
                    log.info("[shutConnect][" + this.taskName + "] " + "shutdown clusterManger.");

                    this.fetchThread.shutdown();
                    log.info("[shutConnect][" + this.taskName + "] " + "shutdown fetch thread start.");
                    this.dispatchThread.shutdown();
                    log.info("[shutConnect][" + this.taskName + "] " + "shutdown dispatch thread start.");
                    this.workThreadCluster.shutdown();
                    log.info("[shutConnect][" + this.taskName + "] " + "shutdown work thread start.");

                    this.fetchThread.awaitShutdown();
                    log.info("[shutConnect][" + this.taskName + "] " + "shutdown fetch thread end.");
                    this.dispatchThread.awaitShutdown();
                    log.info("[shutConnect][" + this.taskName + "] " + "shutdown dispatch thread end.");
                    this.workThreadCluster.awaitShutdown();
                    log.info("[shutConnect][" + this.taskName + "] " + "shutdown work thread end.");

                    RdsCdcConnectorFactory.removeHeldTask(taskName);
                    RdsCdcCheckpointFactory.shutdownTask(taskName);
                    log.info("[shutConnect][" + this.taskName + "] " + "destroy obj in factory.");
                    this.connectorStatus = ConnectorStatus.CLOSED;

                    if (!showdownRequested) {
                        showdownRequested = true;
                        shutdownCondition.signalAll();
                    }
                }
            } finally {
                controlLock.unlock();
            }
        } catch (Throwable t) {
            log.error(
                    // "fail to shutdown ptubes client " + this.taskName,
                    "[shutConnect] fail to shutdown rds cdc client " + this.taskName,
                    t
            );
            throw t;
        }

        log.info(String.format(
                // "A ptubes client (%s) successfully shutdown.",
                "[shutConnect] A rds cdc client (%s) successfully shutdown.",
                this.taskName
        ));
    }

    @Override
    public ConnectorStatus getStatus() {
        return this.connectorStatus;
    }

    public void awaitShutdown() {
        controlLock.lock();

        try {
            while (!showdownRequested) {
                shutdownCondition.awaitUninterruptibly();
            }
        } finally {
            controlLock.unlock();
        }
    }

    public IRdsCdcEventListener getRdsCdcEventListener() {
        return rdsCdcEventListener;
    }

    public void setRdsCdcEventListener(IRdsCdcEventListener rdsCdcEventListener) {
        this.rdsCdcEventListener = rdsCdcEventListener;
    }

    public String getTaskName() {
        return taskName;
    }

    public RdsCdcClientConfigManager getRdsCdcClientConfigManager() {
        return rdsCdcClientConfigManager;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ConnectorMonitorInfo<C> getConnectorMonitorInfo() {
        ConnectorMonitorInfo<C> connectorMonitorInfo = new ConnectorMonitorInfo<>();

        if (!this.connectorStatus.equals(ConnectorStatus.RUNNING)) {
            return connectorMonitorInfo;
        }

        connectorMonitorInfo.setStartTimestamp(this.startTs);
        connectorMonitorInfo.setTaskName(this.taskName);
        connectorMonitorInfo.setFetchMonitorInfo(fetchThread.getFetchMonitorInfo());

        Map<String, WorkMonitorInfo<C>> workMonitorInfoMap = new HashMap<>();
        for (Map.Entry<Integer, WorkThread> entry : workThreadCluster.getWorkThreadMap()
                .entrySet()) {
            workMonitorInfoMap.put(
                    entry.getValue()
                            .getName(),
                    (WorkMonitorInfo<C>) entry.getValue()
                            .getWorkMonitorInfo()
            );
        }

        connectorMonitorInfo.setWorkMonitorInfoMap(workMonitorInfoMap);
        return connectorMonitorInfo;
    }

    @Override
    public AckStatus ack(String idForAck) {
        if (this.connectorStatus != ConnectorStatus.RUNNING) {
            log.error("[ack] connector status is not running:" + this.connectorStatus);
            return AckStatus.FAIL_CONNECTOR_IS_NOT_RUNNING;
        }
        if (null == idForAck || idForAck.length() < 6 || idForAck.charAt(4) != '-') {
            log.error("[ack] wrong received idForAck:" + idForAck);
            return AckStatus.FAIL_WRONG_ACK_ID;
        }
        int key = Integer.parseInt(idForAck.substring(0, 4));
        try {
            WorkThread workThread = workThreadCluster.getWorkThreadMap().get(key);
            if (null == workThread) {
                log.error("[ack] partition not exist1 idForAck:" + idForAck + "; partition:" + key);
                return AckStatus.FAIL_NOT_EXISTENT_PARTITION;
            }
            return workThread.ack(idForAck);
        } catch (NullPointerException e) {
            log.error("[ack] partition not exist2 idForAck:" + idForAck + "; partition:" + key, e);
            return AckStatus.FAIL_NOT_EXISTENT_PARTITION;
        } catch (Exception e) {
            log.error("[ack] fail unknown exception idForAck:" + idForAck + "; partition:" + key);
            return AckStatus.FAIL_WITH_EXCEPTION;
        }
    }

    @Override
    public Logger getLog() {
        return this.log;
    }
}
