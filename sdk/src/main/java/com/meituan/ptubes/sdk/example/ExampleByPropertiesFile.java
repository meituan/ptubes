package com.meituan.ptubes.sdk.example;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.sdk.PtubesConnector;
import com.meituan.ptubes.sdk.IRdsCdcEventListener;
import com.meituan.ptubes.sdk.RdsCdcEventStatus;
import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.sdk.config.notification.SimpleLocalFileConfigChangeNotifier;
import com.meituan.ptubes.sdk.constants.SdkConstants;
import java.util.List;
import java.util.Set;
import com.meituan.ptubes.common.utils.PbJsonUtil;
import com.meituan.ptubes.sdk.protocol.RdsPacket;
import com.meituan.ptubes.sdk.utils.CommonUtil;

public class ExampleByPropertiesFile {
    public static void main(String[] args) throws Exception {
        String sysTaskConfigPath = System.getProperty(SdkConstants.SYS_PROPERTY_CONF_DIR);
        // load task set.
        Set<String> strings = CommonUtil.buildSdkTaskSet(sysTaskConfigPath);
        for (String taskName : strings) {
            SimpleLocalFileConfigChangeNotifier simpleLocalFileConfigChangeNotifier = null;

            // initial task config path and ConfigChangeNotifier.
            if (sysTaskConfigPath != null && !sysTaskConfigPath.isEmpty()) {
                String taskConfigPath = sysTaskConfigPath.substring(
                        0, sysTaskConfigPath.length() - SdkConstants.SDK_CONFIG_SET_FILE_NAME.length()) + taskName + ".properties";
                simpleLocalFileConfigChangeNotifier = new SimpleLocalFileConfigChangeNotifier(taskName, taskConfigPath);
            } else {
                simpleLocalFileConfigChangeNotifier = new SimpleLocalFileConfigChangeNotifier(taskName);
            }
            MyRdsCdcEventListener2 myRdsCdcEventListener2 = new MyRdsCdcEventListener2();

            // 1. create a task
            PtubesConnector<MysqlCheckpoint> rdsCdcConnector = PtubesConnector.newMySQLConnection(taskName, simpleLocalFileConfigChangeNotifier, myRdsCdcEventListener2);
            // use the log same with ptubes task in event listener
            myRdsCdcEventListener2.setLog(rdsCdcConnector.getLog());

            // 2. start a task
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        // synchronously blocking invoke.
                        rdsCdcConnector.startup();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        boolean stop = false;
        while (!stop) {
            Thread.sleep(2000L);
        }
        System.exit(0);
    }

    public static class MyRdsCdcEventListener2 implements IRdsCdcEventListener {
        private Logger log = null;

        @Override
        public RdsCdcEventStatus onEvents(List<RdsPacket.RdsEvent> events) {
            for (RdsPacket.RdsEvent event : events) {
                if (null != log) {
                    log.info("[caught data] event type is " + event.getEventType());
                    log.info("[caught data] header is " + PbJsonUtil.printToStringDefaultNull(event.getHeader()));
                    log.info("[caught data] body is " + PbJsonUtil.printToStringDefaultNull(event.getRowData()));
                }
            }
            return RdsCdcEventStatus.SUCCESS;
        }

        public void setLog(Logger log) {
            this.log = log;
        }
    }
}
