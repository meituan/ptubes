package com.meituan.ptubes.sdk.config.notification;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.model.ReaderInfo;
import com.meituan.ptubes.sdk.model.ReaderServerInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.meituan.ptubes.common.utils.PropertiesUtils;
import com.meituan.ptubes.sdk.config.PtubesSdkConsumerConfig;
import com.meituan.ptubes.sdk.config.PtubesSdkSubscriptionConfig;

public class SimpleLocalFileConfigChangeNotifier implements IConfigChangeNotifier {
    private final Logger log;
    private final String taskName;
    private final String propertyFilePath;
    private static final String TASK_CONFIG_FILE = PropertiesUtils.CLASSPATH_URL_PREFIX + "%s.properties";

    public SimpleLocalFileConfigChangeNotifier(String taskName) {
        this(taskName, String.format(TASK_CONFIG_FILE, taskName));
    }

    public SimpleLocalFileConfigChangeNotifier(String taskName, String propertyFilePath) {
        this.log = LoggerFactory.getLoggerByTask(SimpleLocalFileConfigChangeNotifier.class, taskName);
        this.taskName = taskName;
        this.propertyFilePath = propertyFilePath;
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

        Properties properties = PropertiesUtils.getPropertiesByFile(propertyFilePath);

        return new PtubesSdkSubscriptionConfig(
                properties.getProperty("ptubes.sdk.reader.name"),
                properties.getProperty("ptubes.sdk.task.name"),
                properties.getProperty("ptubes.sdk.zookeeper.address"),
                properties.getProperty("ptubes.sdk.subs")
        );
    }

    public PtubesSdkConsumerConfig getInitConsumerConfig() {
        return new PtubesSdkConsumerConfig();
    }

    public ReaderInfo getReaderInfo() {
        Properties properties = PropertiesUtils.getPropertiesByFile(propertyFilePath);
        String ipPorts = properties.getProperty("ptubes.sdk.reader.ip");
        List<ReaderServerInfo> readers = new ArrayList<>();
        for (String ipPort : ipPorts.split(",")) {
            readers.add(new ReaderServerInfo(ipPort.split(":")[0], Integer.parseInt(ipPort.split(":")[1])));
        }
        return new ReaderInfo(readers);
    }
}
