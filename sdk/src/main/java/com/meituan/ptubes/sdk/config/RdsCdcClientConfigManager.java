package com.meituan.ptubes.sdk.config;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.AckType;
import com.meituan.ptubes.sdk.checkpoint.RdsCdcCheckpointFactory;
import com.meituan.ptubes.sdk.config.notification.DefaultConfChangedRecipient;
import com.meituan.ptubes.sdk.config.notification.IConfigChangeNotifier;
import com.meituan.ptubes.sdk.model.PartitionClusterInfo;

import java.util.HashSet;

public class RdsCdcClientConfigManager {

    private static Logger log;

    private final String taskName;
    private final IConfigChangeNotifier configChangeNotifier;
    private final DefaultConfChangedRecipient defaultConfChangedRecipient;
    private RdsCdcSourceType sourceType;

    private int totalPartitionNum;
    private String readerAppkey;

    private RdsCdcClusterConfig clusterConfig;
    private PtubesSdkConsumerConfig consumerConfig;
    private PtubesSdkSubscriptionConfig subscriptionConfig;
    private FetchThreadConfig fetchThreadConfig;
    private WorkThreadConfig workThreadConfig;

    public RdsCdcClientConfigManager(String taskName, IConfigChangeNotifier notifier) {
        this.taskName = taskName;
        this.configChangeNotifier = notifier;
        this.defaultConfChangedRecipient = new DefaultConfChangedRecipient(taskName);
        RdsCdcClientConfigManager.log = LoggerFactory.getLoggerByTask(
            RdsCdcClientConfigManager.class,
            taskName
        );
    }

    public void init() {
        this.getConfigChangeNotifier().setConfChangedRecipient(defaultConfChangedRecipient);

        this.consumerConfig = configChangeNotifier.getConfig(null, PtubesSdkConsumerConfig.class);
        this.subscriptionConfig = configChangeNotifier.getConfig(null, PtubesSdkSubscriptionConfig.class);

        this.totalPartitionNum = subscriptionConfig.getPartitionNum();
        this.readerAppkey = subscriptionConfig.getReaderAppkey();
        this.sourceType = null == subscriptionConfig.getSourceType() ? RdsCdcSourceType.MYSQL : subscriptionConfig.getSourceType();

        this.clusterConfig = buildClusterConfig(subscriptionConfig, configChangeNotifier);
        this.workThreadConfig = buildWorkThreadConfig(consumerConfig);
        this.fetchThreadConfig = buildFetchThreadConfig(subscriptionConfig, configChangeNotifier);
    }

    private RdsCdcClusterConfig buildClusterConfig(PtubesSdkSubscriptionConfig subscriptionConfig, IConfigChangeNotifier configChangeNotifier) {
        RdsCdcClusterConfig rdsCdcClusterConfig = new RdsCdcClusterConfig();
        rdsCdcClusterConfig.setInitCheckpoint(RdsCdcCheckpointFactory.buildFromLionCheckpoint(
            taskName,
            subscriptionConfig.getBuffaloCheckpoint()
        ));
        rdsCdcClusterConfig.setZkAddr(subscriptionConfig.getRouterAddress());
        rdsCdcClusterConfig.setRdsCdcSourceType(null == subscriptionConfig.getSourceType() ? RdsCdcSourceType.MYSQL : subscriptionConfig.getSourceType());
        rdsCdcClusterConfig.setTaskName(taskName);
        rdsCdcClusterConfig.setNumPartitions(subscriptionConfig.getPartitionNum());
        rdsCdcClusterConfig.setZkSessionTimeoutMs(60000);
        rdsCdcClusterConfig.setZkConnectionTimeoutMs(30000);
        rdsCdcClusterConfig.setMaxDisconnectThreshold(100);
        rdsCdcClusterConfig.setConfigChangeNotifier(configChangeNotifier);

        return rdsCdcClusterConfig;
    }

    private WorkThreadConfig buildWorkThreadConfig(
        PtubesSdkConsumerConfig consumerConfig
    ) {
        return buildWorkThreadConfig(
            taskName,
            sourceType,
            consumerConfig
        );
    }

    private FetchThreadConfig buildFetchThreadConfig(
        PtubesSdkSubscriptionConfig subscriptionConfig,
        IConfigChangeNotifier configChangeNotifier
    ) {
        FetchThreadConfig fetchThreadConfig = new FetchThreadConfig();
        fetchThreadConfig.setFetchBatchSize(subscriptionConfig.getFetchBatchSize());
        fetchThreadConfig.setFetchMaxByteSize(subscriptionConfig.getFetchMaxByteSize());
        fetchThreadConfig.setFetchTimeoutMs(subscriptionConfig.getFetchTimeoutMs());
        fetchThreadConfig.setPartitionNum(subscriptionConfig.getPartitionNum());
        fetchThreadConfig.setReaderAppkey(subscriptionConfig.getReaderAppkey());
        fetchThreadConfig.setConfigChangeNotifier(configChangeNotifier);
        fetchThreadConfig.setSourceType(null == subscriptionConfig.getSourceType()
                ? RdsCdcSourceType.MYSQL : subscriptionConfig.getSourceType());

        ReaderConnectionConfig readerConnectionConfig = new ReaderConnectionConfig();
        readerConnectionConfig.setServiceGroupInfo(subscriptionConfig.getServiceGroupInfo());
        readerConnectionConfig.setBuffaloCheckpoint(
                RdsCdcCheckpointFactory.buildFromLionCheckpoint(
                    taskName,
                    subscriptionConfig.getBuffaloCheckpoint()
                ));
        readerConnectionConfig.setNeedDDL(subscriptionConfig.isNeedDDL());
        readerConnectionConfig.setNeedEndTransaction(subscriptionConfig.isNeedEndTransaction());

        PartitionClusterInfo partitionClusterInfo = new PartitionClusterInfo();
        partitionClusterInfo.setPartitionTotal(subscriptionConfig.getPartitionNum());
        partitionClusterInfo.setPartitionSet(new HashSet<>());

        readerConnectionConfig.setPartitionClusterInfo(partitionClusterInfo);

        fetchThreadConfig.setReaderConnectionConfig(readerConnectionConfig);

        return fetchThreadConfig;
    }

    public static WorkThreadConfig buildWorkThreadConfig(
        String taskName,
        RdsCdcSourceType sourceType,
        PtubesSdkConsumerConfig consumerConfig
    ) {
        WorkThreadConfig workThreadConfig = new WorkThreadConfig();
        workThreadConfig.setTaskName(taskName);
        workThreadConfig.setSourceType(sourceType);
        // Default not ACK
        workThreadConfig.setAckType(null == consumerConfig.getAckType() ? AckType.WITHOUT_ACK : consumerConfig.getAckType());
        workThreadConfig.setBatchSize(consumerConfig.getBatchConsumeSize());
        workThreadConfig.setBatchTimeoutMs(consumerConfig.getBatchConsumeTimeoutMs());
        workThreadConfig.setWorkerTimeoutMs(consumerConfig.getWorkerTimeoutMs());
        workThreadConfig.setCheckpointSyncIntervalMs(consumerConfig.getCheckpointSyncIntervalMs());
        workThreadConfig.setConsumptionMode(PtubesSdkConsumerConfig.ConsumptionMode.valueOf(consumerConfig.getConsumptionMode()));
        workThreadConfig.setFailureMode(PtubesSdkConsumerConfig.FailureMode.valueOf(consumerConfig.getFailureMode()));
        workThreadConfig.setRetryTimes(consumerConfig.getRetryTimes());
        workThreadConfig.setQpsLimit(consumerConfig.getQpsLimit());

        return workThreadConfig;
    }

    public String getTaskName() {
        return taskName;
    }

    public RdsCdcSourceType getSourceType() {
        return sourceType;
    }

    public WorkThreadConfig getWorkThreadConfig() {
        return workThreadConfig;
    }

    public int getTotalPartitionNum() {
        return totalPartitionNum;
    }

    public RdsCdcClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    public PtubesSdkConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }

    public PtubesSdkSubscriptionConfig getSubscriptionConfig() {
        return subscriptionConfig;
    }

    public FetchThreadConfig getFetchThreadConfig() {
        return fetchThreadConfig;
    }

    public String getReaderAppkey() {
        return readerAppkey;
    }

    public IConfigChangeNotifier getConfigChangeNotifier() {
        return configChangeNotifier;
    }

    public DefaultConfChangedRecipient getDefaultConfChangedRecipient() {
        return defaultConfChangedRecipient;
    }
}
