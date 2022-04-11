package com.meituan.ptubes.sdk.config.notification;

import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterManager;
import com.meituan.ptubes.sdk.config.RdsCdcClientConfigManager;
import com.meituan.ptubes.sdk.config.PtubesSdkConsumerConfig;
import com.meituan.ptubes.sdk.config.PtubesSdkSubscriptionConfig;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;
import com.meituan.ptubes.sdk.consumer.FetchThread;
import com.meituan.ptubes.sdk.consumer.WorkThreadCluster;
import org.apache.commons.lang3.tuple.Pair;

/**
 * this recipient will be invoked when config is changed by user function
 *
 */
public class DefaultConfChangedRecipient {
    private final String taskName;

    // consume
    private WorkThreadCluster workThreadCluster;
    private RdsCdcSourceType sourceType;

    // sub
    private FetchThread fetchThread;
    private IRdsCdcClusterManager clusterManager;

    public DefaultConfChangedRecipient(String taskName) {
        this.taskName = taskName;
    }

    public void init(WorkThreadCluster workThreadCluster, FetchThread fetchThread, IRdsCdcClusterManager clusterManager) {
        this.workThreadCluster = workThreadCluster;
        this.fetchThread = fetchThread;
        this.clusterManager = clusterManager;

        this.sourceType = fetchThread.getSourceType();
    }

    /**
     * @param rdsCdcClientSubscriptionConfigPair Pair<new,old>
     */
    public synchronized void onSubscriptionConfigChanged(Pair<PtubesSdkSubscriptionConfig, PtubesSdkSubscriptionConfig> rdsCdcClientSubscriptionConfigPair) {
        PtubesSdkSubscriptionConfig oldConfig = rdsCdcClientSubscriptionConfigPair.getLeft();

        PtubesSdkSubscriptionConfig config = rdsCdcClientSubscriptionConfigPair.getRight();

        if (oldConfig == null || config == null) {
            return;
        }

        fetchThread.updateConnectionConfig(
                config.getServiceGroupInfo(),
                config.isNeedEndTransaction(),
                config.isNeedDDL()
        );
        fetchThread.updateCurrentCheckpoint(
                config.getBuffaloCheckpoint(),
                true,
                false
        );
        fetchThread.updateFetchConfig(
                config.getFetchBatchSize(),
                config.getFetchTimeoutMs(),
                config.getFetchMaxByteSize()
        );

        if (oldConfig.getPartitionNum() != config.getPartitionNum()) {
            this.clusterManager.updatePartitionNum(config.getPartitionNum());
        }

        if (!oldConfig.getReaderAppkey()
                .equals(config.getReaderAppkey())) {
            fetchThread.setNeedRebalance(true);
        } else {
            fetchThread.setNeedResubscribe(true);
        }
    }

    /**
     * @param rdsCdcClientConsumerConfigPair Pair<new,old>
     */
    public synchronized void onConsumerConfigChanged(Pair<PtubesSdkConsumerConfig, PtubesSdkConsumerConfig> rdsCdcClientConsumerConfigPair) {
        PtubesSdkConsumerConfig oldConfig = rdsCdcClientConsumerConfigPair.getLeft();

        PtubesSdkConsumerConfig config = rdsCdcClientConsumerConfigPair.getRight();

        if (oldConfig == null || config == null || oldConfig.equals(config)) {
            return;
        }

        this.workThreadCluster.updateWorkThreadConfig(RdsCdcClientConfigManager.buildWorkThreadConfig(
                taskName,
                sourceType,
                config
        ));
    }
}
