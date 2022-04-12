package com.meituan.ptubes.sdk;

import java.util.HashSet;
import java.util.Set;
import com.meituan.ptubes.sdk.config.RdsCdcClientConfigManager;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;
import com.meituan.ptubes.sdk.config.notification.IConfigChangeNotifier;

public class RdsCdcConnectorFactory {

    private static Set<String> taskNames = new HashSet<>();

    private static IPtubesConnector buildControllableConnector(
            String taskName, IConfigChangeNotifier iConfigChangeNotifier, IRdsCdcEventListener iRdsCdcEventListener) throws Exception {
        // There can only be one client task with one task in a single classLoader
        if (taskNames.contains(taskName)) {
            throw new RuntimeException("Task name: " + taskName + " client sdk has already initialized on this jvm.");
        }

        RdsCdcClientConfigManager rdsCdcClientConfigManager = new RdsCdcClientConfigManager(taskName, iConfigChangeNotifier); // FIXME: 2022/2/16
        rdsCdcClientConfigManager.init();
        PtubesConnector clusterRdsCdcConnector = null;
        if (null == rdsCdcClientConfigManager.getSourceType() || RdsCdcSourceType.MYSQL == rdsCdcClientConfigManager.getSourceType()) {
            clusterRdsCdcConnector = new MysqlConnector(taskName, rdsCdcClientConfigManager);
        }else {
            throw new RuntimeException("unknown sourceType in buildControllableConnector:" + rdsCdcClientConfigManager.getSourceType());
        }
        clusterRdsCdcConnector.registerEventListener(iRdsCdcEventListener);
        taskNames.add(taskName);

        return clusterRdsCdcConnector;
    }

    public synchronized static IPtubesConnector buildMySQLConnector(
            String taskName, IConfigChangeNotifier iConfigChangeNotifier, IRdsCdcEventListener iRdsCdcEventListener) throws Exception {
        return buildControllableConnector(taskName, iConfigChangeNotifier, iRdsCdcEventListener);
    }

    public synchronized static void removeHeldTask(String taskName) {
        taskNames.remove(taskName);
    }
}
