package com.meituan.ptubes.sdk.example;

import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.PtubesConnector;
import com.meituan.ptubes.sdk.ConnectorStatus;
import com.meituan.ptubes.sdk.IRdsCdcEventListener;
import com.meituan.ptubes.sdk.RdsCdcEventStatus;
import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.sdk.config.notification.DefaultConfChangedRecipient;
import com.meituan.ptubes.sdk.config.notification.IConfigChangeNotifier;
import com.meituan.ptubes.sdk.model.ReaderInfo;
import com.meituan.ptubes.sdk.model.ReaderServerInfo;
import com.meituan.ptubes.sdk.monitor.ConnectorMonitorInfo;
import com.meituan.ptubes.sdk.monitor.WorkMonitorInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.meituan.ptubes.common.utils.PbJsonUtil;
import com.meituan.ptubes.sdk.config.PtubesSdkConsumerConfig;
import com.meituan.ptubes.sdk.config.PtubesSdkSubscriptionConfig;
import com.meituan.ptubes.sdk.protocol.RdsPacket;

public class ExampleByMainFunction {
    public static String taskName = "ptubes_demo_main_task";

    public static void main(String[] args) throws Exception {

        // set log file directory
        System.setProperty(LoggerFactory.DEFAULT_LOG_DIR_PROPERTY, System.getProperty("user.dir")  + "/logs");

        // set log type
        System.setProperty(LoggerFactory.DEFAULT_LOG_TYPE_PROPERTY, "log4j2");

        // 1. create a task
        PtubesConnector<MysqlCheckpoint> mysqlConnector = PtubesConnector.newMySQLConnection(taskName, new MysqlConfigNotifier(), new MysqlEventListener1());

        // 2. start a task
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // synchronously blocking invoke.
                    mysqlConnector.startup();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        // 3. shutdown a task
        // rdsCdcConnector.shutdown();

        // tips: hot to use a runtime monitor
        while (ConnectorStatus.CLOSED != mysqlConnector.getStatus()) {
            Thread.sleep(10000L);
            ConnectorMonitorInfo<MysqlCheckpoint> connectorMonitorInfo = mysqlConnector.getConnectorMonitorInfo();

            // partition number to work thread monitor.
            Map<String, WorkMonitorInfo<MysqlCheckpoint>> workMonitorInfoMap = connectorMonitorInfo.getWorkMonitorInfoMap();

            // partition 0 monitor info (might be empty during initialization state)
            WorkMonitorInfo<MysqlCheckpoint> partition0MonitorInfo = workMonitorInfoMap.get("rds-cdc-worker-" + taskName + "-0");

            //
            if (null != partition0MonitorInfo) {

                // a log print by this logger will be logged in the file which same with the ptubes task logs.
                mysqlConnector.getLog().info("latest heart beat timestamp: " + partition0MonitorInfo.getHeartbeatTimestamp());

                System.out.println("latest heart beat timestamp: " + partition0MonitorInfo.getHeartbeatTimestamp());
                System.out.println("latest heart beat checkpoint: " + partition0MonitorInfo.getHeartbeatTimestamp());
                System.out.println("latest common binlog event checkpoint: " + partition0MonitorInfo.getLatestCheckpoint());

            }
        }
        System.exit(0);
    }

    public static class MysqlConfigNotifier implements IConfigChangeNotifier {
        @Override
        @SuppressWarnings("unchecked")
        public <T> T getConfig(String confName, Class<T> confClass) {
            // ignore confName, when there is only one implement for subscript or consumer config in task.
            try {
                if (confClass.isAssignableFrom(PtubesSdkConsumerConfig.class)) {
                    return (T) new PtubesSdkConsumerConfig();
                }
                if (confClass.isAssignableFrom(PtubesSdkSubscriptionConfig.class)) {
                    return (T) new PtubesSdkSubscriptionConfig(
                        "dbustest2-5",
                        taskName,
                        "127.0.0.1:2181",
                        "ptubes_db.ptubes_test_table,ptubes_db2.ptubes_test_table2"
                    );
                }
                if (confClass.isAssignableFrom(ReaderInfo.class)) {
                    List<ReaderServerInfo> readers = new ArrayList<>();
                    ReaderServerInfo readerServerInfo = new ReaderServerInfo("127.0.0.1", 28332);
                    readers.add(readerServerInfo);
                    return (T) new ReaderInfo(readers);
                }

            } catch (Exception e) {
                System.out.println("getConfig error for confName:{" + confName + "}, confClass:{" + confClass + "}");
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public void registerAllListener() {

        }

        @Override
        public void deRegisterAllListener() {

        }

        @Override
        public void setConfChangedRecipient(DefaultConfChangedRecipient defaultConfChangedRecipient) {

        }

        @Override
        public void shutdown() {

        }
    }

    public static class MysqlEventListener1 implements IRdsCdcEventListener {
        @Override
        public RdsCdcEventStatus onEvents(List<RdsPacket.RdsEvent> events) {
            for (RdsPacket.RdsEvent event : events) {
                System.out.println("[caught data] event type is " + event.getEventType());
                System.out.println("[caught data] header is " + PbJsonUtil.printToStringDefaultNull(event.getHeader()));
                System.out.println("[caught data] body is " + PbJsonUtil.printToStringDefaultNull(event.getRowData()));
            }
            return RdsCdcEventStatus.SUCCESS;
        }
    }

}
