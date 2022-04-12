package com.meituan.ptubes.sdk.config.notification;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.model.ReaderInfo;
import java.util.Properties;
import com.meituan.ptubes.common.utils.JacksonUtil;
import com.meituan.ptubes.common.utils.PropertiesUtils;
import com.meituan.ptubes.sdk.config.PtubesSdkConsumerConfig;
import com.meituan.ptubes.sdk.config.PtubesSdkSubscriptionConfig;
import com.meituan.ptubes.sdk.config.ReaderClusterConfig;

public class LocalFileConfigChangeNotifier implements IConfigChangeNotifier {
    private final Logger log;
    private final String taskName;
    private PtubesSdkConsumerConfig consumerConfig;
    private PtubesSdkSubscriptionConfig subscriptionConfig;

    private static final String PTUBES_CLIENT_FORMAT = "ptubes-client.%s";
    private static final String CONSUMER_CONFIG_NAME = "consumer-config";
    private static final String SUBSCRIPTION_CONFIG_NAME = "subscription-config";
    private static final String TASK_CONFIG_FILE = "%s.properties";

    public LocalFileConfigChangeNotifier(String taskName) {
        this.log = LoggerFactory.getLoggerByTask(LocalFileConfigChangeNotifier.class, taskName);
        this.taskName = taskName;
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
        ; // no listener
    }

    @Override
    public void deRegisterAllListener() {
        ; // no listener
    }

    @Override
    public void setConfChangedRecipient(DefaultConfChangedRecipient defaultConfChangedRecipient) {
        ; // unnecessary
    }

    @Override
    public void shutdown() {
        ; // unnecessary
    }

    public PtubesSdkSubscriptionConfig getInitSubscriptionConfig() {

        String subscriptionConfigKey = String.format(
                PTUBES_CLIENT_FORMAT,
                SUBSCRIPTION_CONFIG_NAME
        );
        String configFile = String.format(TASK_CONFIG_FILE, taskName);
        Properties properties = PropertiesUtils.getProperties(configFile);
        String subscriptionConfigString = properties.getProperty(subscriptionConfigKey);
        this.subscriptionConfig = JacksonUtil.fromJson(subscriptionConfigString, PtubesSdkSubscriptionConfig.class);
        return subscriptionConfig;
    }

    public PtubesSdkConsumerConfig getInitConsumerConfig() {
        String consumerConfigKey = String.format(
                PTUBES_CLIENT_FORMAT,
                CONSUMER_CONFIG_NAME
        );

        String configFile = String.format(TASK_CONFIG_FILE, taskName);
        Properties properties = PropertiesUtils.getProperties(configFile);

        String consumerConfigString = properties.getProperty(consumerConfigKey);
        this.consumerConfig = JacksonUtil.fromJson(consumerConfigString, PtubesSdkConsumerConfig.class);
        return consumerConfig;
    }

    public ReaderInfo getReaderInfo() {
        return new ReaderInfo(ReaderClusterConfig.getReaderCluster());
    }
}
