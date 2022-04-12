package com.meituan.ptubes.sdk.config.notification;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.model.ReaderInfo;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import com.meituan.ptubes.common.utils.JacksonUtil;
import com.meituan.ptubes.sdk.config.PtubesSdkConsumerConfig;
import com.meituan.ptubes.sdk.config.PtubesSdkSubscriptionConfig;
import org.apache.commons.lang3.tuple.Pair;


public class ZookeeperConfigChangeNotifier implements IConfigChangeNotifier {
    public static final String SDK_CONFIG_PREFIX = "/ptubes-conf";
    private final Logger log;
    private final String taskName;
    private final String consumerConfigPath;
    private final String subscriptionConfigPath;
    private final String serverListPath;
    private ZkClient zkClient;
    private DefaultConfChangedRecipient defaultConfChangedRecipient;
    private PtubesSdkSubscriptionConfig nowSubscriptionConfig;
    private PtubesSdkConsumerConfig nowConsumerConfig;
    private ReaderInfo nowReaderInfo;

    public ZookeeperConfigChangeNotifier(String taskName, String address) {
        this(taskName, address, 60000, 30000);
    }

    public ZookeeperConfigChangeNotifier(String taskName, String address, int sessionTimeout, int connectionTimeout) {
        this.taskName = taskName;
        this.log = LoggerFactory.getLoggerByTask(ZookeeperConfigChangeNotifier.class, taskName);

        zkClient = new ZkClient(address, sessionTimeout, connectionTimeout, new DefaultZkUTF8Serializer());
        consumerConfigPath = String.format("%s/%s/meta/sdk/consumer-config", SDK_CONFIG_PREFIX, taskName);
        subscriptionConfigPath = String.format("%s/%s/meta/sdk/subscription-config", SDK_CONFIG_PREFIX, taskName);
        serverListPath = String.format("%s/%s/server/reader", SDK_CONFIG_PREFIX, taskName);
        System.out.println(consumerConfigPath);
        System.out.println(subscriptionConfigPath);
        System.out.println(serverListPath);
    }

    public ZookeeperConfigChangeNotifier(String taskName, ZkClient zkClient) {
        this.taskName = taskName;
        this.log = LoggerFactory.getLoggerByTask(ZookeeperConfigChangeNotifier.class, taskName);
        this.zkClient = zkClient;
        consumerConfigPath = String.format("%s/%s/meta/sdk/consumer-config", SDK_CONFIG_PREFIX, taskName);
        subscriptionConfigPath = String.format("%s/%s/meta/sdk/subscription-config", SDK_CONFIG_PREFIX, taskName);
        serverListPath = String.format("%s/%s/server/reader", SDK_CONFIG_PREFIX, taskName);
        System.out.println(consumerConfigPath);
        System.out.println(subscriptionConfigPath);
        System.out.println(serverListPath);
    }


    public PtubesSdkSubscriptionConfig getInitSubscriptionConfig() {
        PtubesSdkSubscriptionConfig ptubesSdkSubscriptionConfig = readData(subscriptionConfigPath, PtubesSdkSubscriptionConfig.class);
        this.nowSubscriptionConfig = ptubesSdkSubscriptionConfig;
        return ptubesSdkSubscriptionConfig;
    }

    public PtubesSdkConsumerConfig getInitConsumerConfig() {
        PtubesSdkConsumerConfig ptubesSdkConsumerConfig = readData(consumerConfigPath, PtubesSdkConsumerConfig.class);
        this.nowConsumerConfig = ptubesSdkConsumerConfig;
        return ptubesSdkConsumerConfig;
    }

    public ReaderInfo getReaderInfo() {
        ReaderInfo readerInfo = readData(serverListPath, ReaderInfo.class);
        this.nowReaderInfo = readerInfo;
        return readerInfo;
    }

    @Override
    public <T> T getConfig(String confName, Class<T> confClass) {
        // ignore confName, because there is only one implement for subscript or consumer config in ptubes SDK.
        try {
            if (confClass.isAssignableFrom(PtubesSdkConsumerConfig.class)) {
                return (T) getInitConsumerConfig();
            }
            if (confClass.isAssignableFrom(PtubesSdkSubscriptionConfig.class)) {
                return (T) getInitSubscriptionConfig();
            }
            if (confClass.isAssignableFrom(ReaderInfo.class)) {
                return (T) getReaderInfo();
            }

        } catch (Exception e) {
            log.error("getConfig error for confName:{" + confName + "}, confClass:{" + confClass + "}", e);
        }
        return null;
    }

    @Override
    public void registerAllListener() {
        zkClient.subscribeDataChanges(subscriptionConfigPath, new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                PtubesSdkSubscriptionConfig newPtubesSdkSubscriptionConfig = deserializeByString(data.toString(), PtubesSdkSubscriptionConfig.class);
                PtubesSdkSubscriptionConfig oldPtubesSdkSubscriptionConfig = nowSubscriptionConfig;
                nowSubscriptionConfig = newPtubesSdkSubscriptionConfig;
                defaultConfChangedRecipient.onSubscriptionConfigChanged(Pair.of(oldPtubesSdkSubscriptionConfig, newPtubesSdkSubscriptionConfig));
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                ; // log it
            }
        });

        zkClient.subscribeDataChanges(consumerConfigPath, new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                PtubesSdkConsumerConfig newPtubesSdkConsumerConfig = deserializeByString(data.toString(), PtubesSdkConsumerConfig.class);
                PtubesSdkConsumerConfig oldPtubesSdkConsumerConfig = nowConsumerConfig;
                nowConsumerConfig = newPtubesSdkConsumerConfig;
                defaultConfChangedRecipient.onConsumerConfigChanged(Pair.of(oldPtubesSdkConsumerConfig, newPtubesSdkConsumerConfig));
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                ; // log it
            }
        });

    }

    @Override
    public void deRegisterAllListener() {
        zkClient.unsubscribeAll();
    }

    @Override
    public void setConfChangedRecipient(DefaultConfChangedRecipient defaultConfChangedRecipient) {
        this.defaultConfChangedRecipient = defaultConfChangedRecipient;
    }

    @Override
    public void shutdown() {
        if (null != zkClient) {
            zkClient.close();
            zkClient = null;
        }
    }

    private <T> T readData(String path, Class<T> tClass) {
        String data = zkClient.readData(path, true);
        if (null == data) {
            throw new RuntimeException("consumer or subscription config for task:" + taskName + " in path:" + path + "don't exist.");
        }
        return deserializeByString(data, tClass);

    }

    private <T> T deserializeByString(String data, Class<T> tClass) {
        try {
            return JacksonUtil.fromJson(data, tClass);
        } catch (Exception e) {
            throw new RuntimeException("deserialize consumer or subscription config for task:" + taskName + " error, data is" + data, e);
        }
    }

    public String getConsumerConfigPath() {
        return consumerConfigPath;
    }

    public String getSubscriptionConfigPath() {
        return subscriptionConfigPath;
    }

    public String getServerListPath() {
        return serverListPath;
    }
}
