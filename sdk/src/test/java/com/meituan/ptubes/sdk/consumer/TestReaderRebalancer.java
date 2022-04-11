package com.meituan.ptubes.sdk.consumer;

import com.meituan.ptubes.sdk.reader.ContainerConstants;
import com.meituan.ptubes.sdk.reader.ReaderServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;
import com.meituan.ptubes.sdk.config.ReaderConnectionConfig;
import com.meituan.ptubes.sdk.model.ReaderServerInfo;
import com.meituan.ptubes.sdk.model.ServerInfo;
import com.meituan.ptubes.sdk.model.ServiceGroupInfo;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;



public class TestReaderRebalancer {

    protected static final Logger LOG = Logger.getLogger(TestReaderRebalancer.class.getName());

    private static String taskName;
    private static MockReaderRebalancer readerRebalancer;
    private static ReaderServer readerServer;

    private static List<ReaderServerInfo> sgServices;
    private static ServerInfo connectReader;
    private static ReaderConnectionConfig readerConnectionConfig;

    private static final int PORT = 8080;

    private static final int ZERO_LOAD = 0;
    private static final int HALF_LOAD = 50;
    private static final int FULL_LOAD = 100;

    private static List<String> TASK_LIST_WITHOUT_CURRENT_TASK = new ArrayList<String>() {{
        add("task1");
        add("task2");
        add("task3");
    }};

    private static List<String> TASK_LIST_WITH_CURRENT_TASK = new ArrayList<String>() {{
        add("task1");
        add("task2");
        add("clientDemo");
    }};

    private static List<String> TASK_LIST_WITH_MORE_THAN_ONE_CURRENT_TASK = new ArrayList<String>() {{
        add("task1");
        add("task2");
        add("clientDemo");
        add("clientDemo");
    }};

    @BeforeClass
    public static void setUp() {
        taskName = "clientDemo";
        readerRebalancer = new MockReaderRebalancer(taskName);

        sgServices = new ArrayList<>();
        connectReader = new ServerInfo();
        readerConnectionConfig = new ReaderConnectionConfig();

        ServiceGroupInfo serviceGroupInfo = new ServiceGroupInfo();
        serviceGroupInfo.setTaskName(taskName);
        serviceGroupInfo.setServiceGroupName("testServiceGroupName");
        serviceGroupInfo.setDatabaseInfoSet(new HashSet<>());

        readerConnectionConfig.setServiceGroupInfo(serviceGroupInfo);

        readerServer = new ReaderServer();
    }

    @Test
    public void getServerInfo() throws InterruptedException {
        readerServer.start();
        readerRebalancer.testGetServerInfo();
        readerServer.stop();
    }

    @Test
    public void emptySGServices() {
        sgServices = new ArrayList<>();
        connectReader = new ServerInfo();

        ServerInfo targetReader = readerRebalancer.chooseCandidateServer(
                sgServices,
                connectReader,
                readerConnectionConfig
        );
        Assert.assertEquals(
                null,
                targetReader
        );

        connectReader = null;
        targetReader = readerRebalancer.chooseCandidateServer(
                sgServices,
                connectReader,
                readerConnectionConfig
        );
        Assert.assertEquals(
                null,
                targetReader
        );

        sgServices = null;
        targetReader = readerRebalancer.chooseCandidateServer(
                sgServices,
                connectReader,
                readerConnectionConfig
        );
        Assert.assertEquals(
                null,
                targetReader
        );
    }

    @Test
    public void allCondition() {
        sgServices = new ArrayList<>();
        connectReader = new ServerInfo();

        try {
            sgServices.add(newSGService().setIp("1.1.1.10"));
            sgServices.add(newSGService().setIp("1.1.1.11"));
            sgServices.add(newSGService().setIp("1.1.1.12"));
            sgServices.add(newSGService().setIp("1.1.1.13"));
            sgServices.add(newSGService().setIp("1.1.1.14"));
            sgServices.add(newSGService().setIp("1.1.1.15"));
            sgServices.add(newSGService().setIp("1.1.1.16"));
            sgServices.add(newSGService().setIp("1.1.1.17"));
            sgServices.add(newSGService().setIp("1.1.1.18"));
            sgServices.add(newSGService().setIp("1.1.1.19"));
            sgServices.add(newSGService().setIp("1.1.1.20"));
            sgServices.add(newSGService().setIp("1.1.1.21"));

            connectReader.setAddress(new InetSocketAddress(
                    InetAddress.getByName("1.1.1.13"),
                    PORT
            ));

            ServerInfo targetReader = readerRebalancer.chooseCandidateServer(
                    sgServices,
                    connectReader,
                    readerConnectionConfig
            );

            Assert.assertEquals(
                    "1.1.1.10",
                    targetReader.getAddress()
                            .getHostName()
            );
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(
                    e.getMessage(),
                    false
            );
        }
    }

    private ReaderServerInfo newSGService() {
        ReaderServerInfo serverInfo = new ReaderServerInfo();
        serverInfo.setPort(ContainerConstants.DATA_SERVER_PORT);
        return serverInfo;
    }

    private static ServerInfo buildServerInfo(ReaderServerInfo sgService) {

        ServerInfo serverInfo = new ServerInfo();
        serverInfo.setServerStatus(ServerInfo.ServerStatus.AVAILABLE);

        try {
            serverInfo.setAddress(new InetSocketAddress(
                    InetAddress.getByName(sgService.getIp()),
                    PORT
            ));
        } catch (Exception e) {
            e.printStackTrace();
        }

        switch (sgService.getIp()) {
            case "1.1.1.10":
                serverInfo.setLoadFactor(ZERO_LOAD);
                serverInfo.setWriterTaskList(TASK_LIST_WITHOUT_CURRENT_TASK);
                return serverInfo;
            case "1.1.1.11":
                serverInfo.setLoadFactor(HALF_LOAD);
                serverInfo.setWriterTaskList(TASK_LIST_WITHOUT_CURRENT_TASK);
                return serverInfo;
            case "1.1.1.12":
                serverInfo.setLoadFactor(FULL_LOAD);
                serverInfo.setWriterTaskList(TASK_LIST_WITHOUT_CURRENT_TASK);
                return serverInfo;

            case "1.1.1.13":
                serverInfo.setLoadFactor(ZERO_LOAD);
                serverInfo.setWriterTaskList(TASK_LIST_WITH_CURRENT_TASK);
                return serverInfo;
            case "1.1.1.14":
                serverInfo.setLoadFactor(ZERO_LOAD);
                serverInfo.setWriterTaskList(TASK_LIST_WITH_MORE_THAN_ONE_CURRENT_TASK);
                return serverInfo;

            case "1.1.1.15":
                serverInfo.setLoadFactor(HALF_LOAD);
                serverInfo.setWriterTaskList(TASK_LIST_WITH_CURRENT_TASK);
                return serverInfo;

            case "1.1.1.16":
                serverInfo.setLoadFactor(HALF_LOAD);
                serverInfo.setWriterTaskList(TASK_LIST_WITH_MORE_THAN_ONE_CURRENT_TASK);
                return serverInfo;

            case "1.1.1.17":
                serverInfo.setLoadFactor(FULL_LOAD);
                serverInfo.setWriterTaskList(TASK_LIST_WITH_CURRENT_TASK);
                return serverInfo;
            case "1.1.1.18":
                serverInfo.setLoadFactor(FULL_LOAD);
                serverInfo.setWriterTaskList(TASK_LIST_WITH_MORE_THAN_ONE_CURRENT_TASK);
                return serverInfo;

            case "1.1.1.19":
            case "1.1.1.20":
            case "1.1.1.21":
                serverInfo.setServerStatus(ServerInfo.ServerStatus.UNAVAILABLE);
                return serverInfo;
            default:
                return serverInfo;

        }
    }

    public static class MockReaderRebalancer extends ReaderRebalancer {

        public MockReaderRebalancer(String taskName) {
            super(taskName);
        }

        @Override
        public ServerInfo chooseCandidateServer(
                List<ReaderServerInfo> sgServices,
                ServerInfo connectReader,
                ReaderConnectionConfig readerConnectionConfig
        ) {
            return super.chooseCandidateServer(
                    sgServices,
                    connectReader,
                    readerConnectionConfig
            );
        }

        public ServerInfo testGetServerInfo() {
            ReaderServerInfo serverInfo = new ReaderServerInfo();
            serverInfo.setIp(ContainerConstants.LOCALHOST);
            serverInfo.setPort(ContainerConstants.DATA_SERVER_PORT);
            return super.getServerInfo(serverInfo,
                    new ReaderConnectionConfig()
            );
        }

        @Override
        protected ServerInfo getServerInfo(
                ReaderServerInfo sgService,
                ReaderConnectionConfig readerConnectionConfig
        ) {
            return buildServerInfo(sgService);
        }
    }
}
