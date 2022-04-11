package com.meituan.ptubes.sdk.consumer;

import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.CheckpointPersistenceProviderFactory;
import com.meituan.ptubes.sdk.checkpoint.ICheckpointPersistenceProvider;
import com.meituan.ptubes.sdk.checkpoint.RdsCdcCheckpointFactory;
import com.meituan.ptubes.sdk.config.notification.IConfigChangeNotifier;
import com.meituan.ptubes.sdk.constants.ConsumeConstants;
import com.meituan.ptubes.sdk.model.DataRequest;
import com.meituan.ptubes.sdk.model.ReaderInfo;
import com.meituan.ptubes.sdk.model.ReaderServerInfo;
import com.meituan.ptubes.sdk.model.ServerInfo;
import com.meituan.ptubes.sdk.model.ServiceGroupInfo;
import com.meituan.ptubes.sdk.monitor.FetchMonitorInfo;
import com.meituan.ptubes.sdk.netty.NettyHttpRdsCdcReaderConnection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.meituan.ptubes.common.utils.NameThreadFactory;
import com.meituan.ptubes.common.utils.PbJsonUtil;
import com.meituan.ptubes.sdk.config.FetchThreadConfig;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;
import com.meituan.ptubes.sdk.config.ReaderConnectionConfig;
import com.meituan.ptubes.sdk.protocol.RdsPacket;

public class FetchThread extends AbstractActor {

    // private static final String FETCH_THREAD_PREFIX = "ptubes-fetch-";
    private static final String FETCH_THREAD_PREFIX = "rds-cdc-fetch-";

    private String taskName;
    private final RdsCdcSourceType sourceType;
    private long lastFetchTime; // Used to count time-consuming management Rough statistics temporarily out of sync
    private long fetchEventCount;
    private long fetchFrequencyCount;

    private volatile boolean needRebalance = false;
    private volatile boolean needResubscribe = false;

    private volatile boolean checkpointUpdated = false;
    private final Object checkpointLock = new Object();

    private ReaderConnectionConfig readerConnectionConfig;
    private AtomicReference<List<ReaderServerInfo>> sgServices = new AtomicReference<>();
    private AtomicReference<ServerInfo> pickReader = new AtomicReference<>();
    private AtomicReference<ServerInfo> connectReader = new AtomicReference<>();

    private AtomicReference<BuffaloCheckpoint> currentCheckpoint = new AtomicReference<>();

    private DispatchThread dispatchThread;
    private FetchThreadState fetchThreadState;
    private FetchThreadConfig fetchThreadConfig;
    private IConfigChangeNotifier configChangeNotifier;

    private ScheduledExecutorService scheduledExecutorService;
    private NettyHttpRdsCdcReaderConnection nettyHttpRdsCdcReaderConnection;

    private IReaderRebalancer readerRebalancer;


    public FetchThread(
        String taskName,
        DispatchThread dispatchThread,
        FetchThreadConfig fetchThreadConfig
    ) {
        super(
            FETCH_THREAD_PREFIX + taskName,
            LoggerFactory.getLoggerByTask(
                FetchThread.class,
                taskName
            )
        );

        this.taskName = taskName;
        this.dispatchThread = dispatchThread;
        this.fetchThreadConfig = fetchThreadConfig;
        this.sourceType = fetchThreadConfig.getSourceType();
        this.readerConnectionConfig = fetchThreadConfig.getReaderConnectionConfig();
        this.configChangeNotifier = fetchThreadConfig.getConfigChangeNotifier();

        this.currentCheckpoint.set(readerConnectionConfig.getBuffaloCheckpoint());

        this.fetchThreadState = new FetchThreadState();
        this.readerRebalancer = new ReaderRebalancer(this.taskName);

        ThreadFactory namedThreadFactory = new NameThreadFactory(
            // "ptubes-lb-" + taskName,
            "rds-cdc-lb-" + taskName,
            false
        );
        scheduledExecutorService = new ScheduledThreadPoolExecutor(
            1,
            namedThreadFactory
        );
    }

    @Override
    protected boolean executeAndChangeState(Object message) {
        boolean success = true;

        if (message instanceof FetchThreadState) {
            if (!Status.RUNNING.equals(this.status)) {
                log.warn("Actor not running: " + message.toString());
            } else {
                FetchThreadState fetchThreadState = (FetchThreadState) message;

                if (log.isDebugEnabled()) {
                    log.debug(String.format(
                        "FetchThread %s current state: %s.",
                        this.getName(),
                        fetchThreadState.getStateId()
                    ));
                }

                switch (fetchThreadState.getStateId()) {
                    case START:
                        break;
                    case PICK_SERVER:
                        doPickReader(fetchThreadState);
                        break;
                    case CONNECT:
                        doConnect(fetchThreadState);
                        break;
                    case CONNECT_SUCCESS:
                        doConnectSuccess(fetchThreadState);
                        break;
                    case CONNECT_FAILURE:
                        doConnectFailure(fetchThreadState);
                        break;
                    case SUBSCRIBE:
                        doSubscribe(fetchThreadState);
                        break;
                    case SUBSCRIBE_SUCCESS:
                        doSubscribeSuccess(fetchThreadState);
                        break;
                    case SUBSCRIBE_FAILURE:
                        doSubscribeFailure(fetchThreadState);
                        break;
                    case FETCH_EVENTS:
                        doFetchEvents(fetchThreadState);
                        break;
                    case FETCH_EVENTS_SUCCESS:
                        doFetchEventsSuccess(fetchThreadState);
                        break;
                    case FETCH_EVENTS_FAILURE:
                        doFetchEventsFailure(fetchThreadState);
                        break;
                    case OFFER_DISPATCH_EVENTS:
                        doDispatchEvents(fetchThreadState);
                        break;
                    default: {
                        log.error(
                            "Unknown state in Fetch thread: " + fetchThreadState.getStateId());
                        success = false;
                        break;
                    }
                }
            }
        } else {
            success = super.executeAndChangeState(message);
        }

        return success;
    }

    private ServerInfo pickReader() {
        ReaderInfo config = configChangeNotifier.getConfig(null, ReaderInfo.class);
        if (null == config || null == config.getReaders() || config.getReaders().isEmpty()) {
            this.sgServices.set(Collections.EMPTY_LIST);
        } else {
            this.sgServices.set(config.getReaders());
        }

        // single threaded state machine, so no synchronization here
        readerConnectionConfig.setBuffaloCheckpoint(currentCheckpoint.get());

        return readerRebalancer.chooseCandidateServer(
            sgServices.get(),
            connectReader.get(),
            readerConnectionConfig
        );
    }

    private void doPickReader(FetchThreadState fetchThreadState) {

        log.info("Picking target server to connect.");

        ServerInfo targetServer = pickReader();
        if (targetServer == null) {
            log.info(String.format(
                "Picking target server fail, will retry in %s seconds.",
                ConsumeConstants.FETCH_PICKREADER_INTERVAL_SEC
            ));
            try {
                TimeUnit.SECONDS.sleep(ConsumeConstants.FETCH_PICKREADER_INTERVAL_SEC);
            } catch (InterruptedException e) {
                log.error(
                    "Pick reader thread sleep interrupted.",
                    e
                );
            }
            fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
        } else {
            log.info("Target server info: " + targetServer.getAddress());
            pickReader.set(targetServer);
            fetchThreadState.setStateId(FetchThreadState.StateId.CONNECT);
        }

        this.enqueueMessage(fetchThreadState);
    }

    private void doConnect(FetchThreadState fetchThreadState) {

        if (log.isDebugEnabled()) {
            log.debug("Fetch thread will close current connection to reconnect.");
        }
        resetConnection();

        this.nettyHttpRdsCdcReaderConnection = new NettyHttpRdsCdcReaderConnection(
            pickReader.get(),
            this
        );

        this.connectReader.set(nettyHttpRdsCdcReaderConnection.connect());
    }

    private void doConnectSuccess(FetchThreadState fetchThreadState) {
        fetchThreadState.setStateId(FetchThreadState.StateId.SUBSCRIBE);
        this.enqueueMessage(fetchThreadState);
    }

    private void doConnectFailure(FetchThreadState fetchThreadState) {
        log.warn(String.format(
            "Connect target server fail, will retry in %s milliseconds.",
            ConsumeConstants.FETCH_CONNECT_EERROR_INTERVAL
        ));
        try {
            TimeUnit.MILLISECONDS.sleep(ConsumeConstants.FETCH_CONNECT_EERROR_INTERVAL);
        } catch (InterruptedException e) {
            log.error(
                "Connect reader error sleep interrupted.",
                e
            );
        }

        fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
        this.enqueueMessage(fetchThreadState);
    }

    private synchronized void doSubscribe(FetchThreadState fetchThreadState) {

        readerConnectionConfig.setBuffaloCheckpoint(currentCheckpoint.get());

        this.nettyHttpRdsCdcReaderConnection
            .subscribe(this.readerConnectionConfig);
    }

    private void doSubscribeSuccess(FetchThreadState fetchThreadState) {
        if (needResubscribe) {
            needResubscribe = false;
            fetchThreadState.setStateId(FetchThreadState.StateId.SUBSCRIBE);
        } else {
            this.checkpointUpdated = false;
            fetchThreadState.setStateId(FetchThreadState.StateId.FETCH_EVENTS);
        }
        this.enqueueMessage(fetchThreadState);
    }

    private void doSubscribeFailure(FetchThreadState fetchThreadState) {
        String errorMessage = String.format(
            "Subscribe reader %s fail, will retry in %s milliseconds.",
            this.connectReader.get()
                .getAddress(),
            ConsumeConstants.FETCH_SUBSCRIBE_EERROR_INTERVAL
        );
        log.error(errorMessage);
        try {
            TimeUnit.MILLISECONDS.sleep(ConsumeConstants.FETCH_SUBSCRIBE_EERROR_INTERVAL);
        } catch (InterruptedException e) {
            log.error(
                "Subscribe reader error sleep interrupted.",
                e
            );
        }

        fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
        this.enqueueMessage(fetchThreadState);
    }

    private void doFetchEvents(FetchThreadState fetchThreadState) {

        DataRequest dataRequest = new DataRequest(
            this.fetchThreadConfig.getFetchBatchSize(),
            this.fetchThreadConfig.getFetchTimeoutMs(),
            this.fetchThreadConfig.getFetchMaxByteSize()
        );

        this.nettyHttpRdsCdcReaderConnection.fetchMessages(
            dataRequest,
            fetchThreadState
        );
    }

    private void doFetchEventsSuccess(FetchThreadState fetchThreadState) {
        fetchThreadState.setStateId(FetchThreadState.StateId.OFFER_DISPATCH_EVENTS);
        this.enqueueMessage(fetchThreadState);
    }

    private void doFetchEventsFailure(FetchThreadState fetchThreadState) {
        fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
        this.enqueueMessage(fetchThreadState);
    }

    private void doDispatchEvents(FetchThreadState fetchThreadState) {
        RdsPacket.RdsEnvelope rdsEnvelope = fetchThreadState.getRdsEnvelope();
        fetchFrequencyCount++;

        long totalEvent = rdsEnvelope.getTotalEventCount();

        if (totalEvent == 0) {
            if (fetchFrequencyCount % ConsumeConstants.FETCH_LOG_INTERVAL == 0) {
                log.info("Fetch empty message, fetch thread will retry fetch.");
            }
            this.fetchThreadState.setStateId(FetchThreadState.StateId.FETCH_EVENTS);
        } else {
            fetchEventCount += totalEvent;
            if (log.isDebugEnabled()) {
                log.debug("Fetch event total count: " + fetchEventCount);
            }

            dispatchThread.addEnvelope(rdsEnvelope);

            if ((null == sourceType || sourceType == RdsCdcSourceType.MYSQL) && (rdsEnvelope.getLatestCheckpoint().getTimestamp() > 0)) {
                synchronized (checkpointLock) {
                    if (!checkpointUpdated && !needResubscribe) {
                        currentCheckpoint.set(RdsCdcCheckpointFactory.buildFromPBCheckpoint(
                                taskName, rdsEnvelope.getLatestCheckpoint()
                        ));
                    }
                }
            }else {
                log.error("unknown sourceType in doDispatchEvents:" + sourceType + "or unavailable timestamp in ckp");
                if (sourceType == RdsCdcSourceType.MYSQL) {
                    log.debug("[MYSQL]ckp in fetch:" + PbJsonUtil.printToStringDefaultNull(rdsEnvelope.getLatestCheckpoint()));
                } else {
                    ; // do noting
                }
            }
        }

        if (needRebalance) {
            log.info(getName() + " need rebalance now.");
            fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);

            needRebalance = false;
            needResubscribe = false;
        } else if (needResubscribe) {
            log.info(getName() + " need resubscribe now.");
            fetchThreadState.setStateId(FetchThreadState.StateId.SUBSCRIBE);

            needResubscribe = false;
        } else {
            fetchThreadState.setStateId(FetchThreadState.StateId.FETCH_EVENTS);
        }

        this.enqueueMessage(fetchThreadState);
    }

    public FetchThreadState getFetchThreadState() {
        return fetchThreadState;
    }

    public boolean isNeedResubscribe() {
        return needResubscribe;
    }

    public void setNeedResubscribe(boolean needResubscribe) {
        this.needResubscribe = needResubscribe;
    }

    public void setNeedRebalance(boolean needRebalance) {
        this.needRebalance = needRebalance;
    }

    @Override
    public void doStart(LifecycleMessage lifecycleMessage) {
        log.info("Fetch thread start.");
        if (!FetchThreadState.StateId.START.equals(fetchThreadState.getStateId())) {
            return;
        }

        super.doStart(lifecycleMessage);
        fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
        enqueueMessage(fetchThreadState);

        ICheckpointPersistenceProvider checkpointPersistenceProvider = CheckpointPersistenceProviderFactory.getInstance(this.taskName);
        int partitionNum = this.readerConnectionConfig.getPartitionClusterInfo()
            .getPartitionTotal();

        BuffaloCheckpoint zkCheckpoint = checkpointPersistenceProvider.getEarliestCheckpoint(partitionNum);
        if (zkCheckpoint != null) {
            currentCheckpoint.set(zkCheckpoint);
        }
        this.scheduledExecutorService.scheduleAtFixedRate(
            new RebalanceTask(),
            new Random().nextInt((int) ConsumeConstants.FETCH_REBALANCE_INTERVAL_MS),
            ConsumeConstants.FETCH_REBALANCE_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
    }

    @Override
    protected void onShutdown() {
        if (null != nettyHttpRdsCdcReaderConnection) {
            log.info("closing netty connection during shutdown");
            nettyHttpRdsCdcReaderConnection.close();
            nettyHttpRdsCdcReaderConnection = null;
        }

        log.info("Shutdown fetch thread scheduled executor service.");
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void resume() {
        super.resume();
        fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
        enqueueMessage(fetchThreadState);
    }

    protected void resetConnection() {
        NettyHttpRdsCdcReaderConnection conn = this.nettyHttpRdsCdcReaderConnection;

        if (null != conn) {
            conn.close();
            this.nettyHttpRdsCdcReaderConnection = null;
        }
    }

    public String getTaskName() {
        return this.taskName;
    }

    public RdsCdcSourceType getSourceType() {
        return sourceType;
    }

    public long getLastFetchTime() {
        return lastFetchTime;
    }

    public void setLastFetchTime(long lastFetchTime) {
        this.lastFetchTime = lastFetchTime;
    }

    private class RebalanceTask extends TimerTask {

        @Override
        public void run() {
            log.info("Running rebalance task.");

            if (pickReader.get() == null) {
                log.info("Client still picking readers, no need to rebalance.");
                return;
            }

            ServerInfo lbsPickReader = pickReader();
            ServerInfo currentConnectReader = connectReader.get();

            if (lbsPickReader == null) {
                log.warn("No available candidate servers now.");
                needRebalance = true;
                return;
            }

            if (!currentConnectReader.equals(lbsPickReader)) {
                needRebalance = true;
            }

            log.info(String.format(
                "Rebalance task finish, need rebalance: %s, target serverInfo: %s",
                needRebalance,
                lbsPickReader
            ));
        }
    }

    public FetchMonitorInfo getFetchMonitorInfo() {
        FetchMonitorInfo fetchMonitorInfo = new FetchMonitorInfo();
        fetchMonitorInfo.setTargetServerInfo(connectReader.get());

        if (nettyHttpRdsCdcReaderConnection != null) {
            fetchMonitorInfo.setInboundBytes(nettyHttpRdsCdcReaderConnection.getInboundBytes());
            fetchMonitorInfo.setInboundBytes(nettyHttpRdsCdcReaderConnection.getOutboundBytes());
        }

        return fetchMonitorInfo;
    }

    public BuffaloCheckpoint getCurrentCheckpoint() {
        return currentCheckpoint.get();
    }


    public synchronized void addPartition(int partitionId) {
        readerConnectionConfig.addPartition(partitionId);
    }

    public synchronized void dropPartition(int partitionId) {
        readerConnectionConfig.removePartition(partitionId);
    }

    public synchronized boolean needUpdatePartitionTotal(int partitionTotal) {
        return readerConnectionConfig.getPartitionClusterInfo()
            .getPartitionTotal() != partitionTotal;
    }

    public synchronized void updatePartitionTotal(int partitionTotal) {
        readerConnectionConfig.getPartitionClusterInfo()
            .setPartitionTotal(partitionTotal);
    }

    // It will be called in three cases: 1, lion adjusts the storage point; 2, after modifying the number of shards, the member node calls back and adjusts to the oldest storage point; 3, if the storage point moved into the shard is smaller than all work threads The storage point needs to be called back;
    public void updateCurrentCheckpoint(
        BuffaloCheckpoint checkpoint,
        boolean onlyCompareVersion,
        boolean forceUpdate
    ) {
        if (checkpoint == null) {
            log.error("checkpoint is null, will not update current checkpoint");
            return;
        }

        log.info(String.format(
            "Current fetch checkpoint info %s, update checkpoint info %s.",
            currentCheckpoint.get(),
            checkpoint
        ));

        synchronized (checkpointLock) {
            if (forceUpdate || (onlyCompareVersion ? checkpoint.getVersionTs() > currentCheckpoint.get()
                .getVersionTs() : checkpoint.compareTo(currentCheckpoint.get()) < 0)) {
                currentCheckpoint.set(RdsCdcCheckpointFactory.buildFromLionCheckpoint(
                    taskName,
                    checkpoint
                ));
                checkpointUpdated = true;

                log.info("Fetch checkpoint has updated." + currentCheckpoint.get());
            }
        }
    }

    public synchronized void updateConnectionConfig(
        ServiceGroupInfo serviceGroupInfo,
        boolean needEndTransaction,
        boolean needDDL
    ) {
        this.readerConnectionConfig.setServiceGroupInfo(serviceGroupInfo);
        this.readerConnectionConfig.setNeedEndTransaction(needEndTransaction);
        this.readerConnectionConfig.setNeedDDL(needDDL);
    }

    public synchronized void updateFetchConfig(
        int fetchBatchSize,
        int fetchTimeoutMs,
        int fetchMaxByteSize
    ) {
        this.fetchThreadConfig.setFetchBatchSize(fetchBatchSize);
        this.fetchThreadConfig.setFetchTimeoutMs(fetchTimeoutMs);
        this.fetchThreadConfig.setFetchMaxByteSize(fetchMaxByteSize);
    }
}
