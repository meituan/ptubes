package com.meituan.ptubes.sdk.netty;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import com.meituan.ptubes.sdk.config.FetchThreadConfig;
import com.meituan.ptubes.sdk.config.RdsCdcClientConfigManager;
import com.meituan.ptubes.sdk.config.ReaderConnectionConfig;
import com.meituan.ptubes.sdk.config.notification.SimpleLocalFileConfigChangeNotifier;
import com.meituan.ptubes.sdk.consumer.FetchThread;
import com.meituan.ptubes.sdk.consumer.FetchThreadState;
import com.meituan.ptubes.sdk.model.DataRequest;
import com.meituan.ptubes.sdk.model.ServerInfo;
import com.meituan.ptubes.sdk.reader.ContainerConstants;
import com.meituan.ptubes.sdk.reader.ReaderServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



public class TestNettyHttpRdsCdcReaderConnection {

    private String taskName;

    private FetchThread fetchThread;
    private FetchThreadConfig fetchThreadConfig;
    private NettyHttpRdsCdcReaderConnection nettyHttpRdsCdcReaderConnection;
    private ReaderServer readerServer;
    private DataRequest dataRequest;


    @Before
    public void setUp() throws UnknownHostException {
        taskName = "clientDemo";

        RdsCdcClientConfigManager rdsCdcClientConfigManager =
                new RdsCdcClientConfigManager(taskName, new SimpleLocalFileConfigChangeNotifier(taskName));
        rdsCdcClientConfigManager.init();

        fetchThread = new FetchThread(
            taskName,
            null,
            rdsCdcClientConfigManager.getFetchThreadConfig()
        );

        readerServer = new ReaderServer();
        readerServer.start();

        ServerInfo targetServer = new ServerInfo();
        targetServer.setAddress(new InetSocketAddress(
            InetAddress.getByName(ContainerConstants.LOCALHOST),
            ContainerConstants.DATA_SERVER_PORT
        ));
        nettyHttpRdsCdcReaderConnection = new NettyHttpRdsCdcReaderConnection(
            targetServer,
            fetchThread
        );

        dataRequest = new DataRequest(
            1000,
            1000,
            1000
        );
    }

    @After
    public void cleanUp() {
        readerServer.stop();
    }

    @Test
    public void test() {
        nettyHttpRdsCdcReaderConnection.connect();
        Assert.assertEquals(
            FetchThreadState.StateId.CONNECT_SUCCESS,
            fetchThread.getFetchThreadState()
                .getStateId()
        );
        nettyHttpRdsCdcReaderConnection.subscribe(new ReaderConnectionConfig());
        Assert.assertEquals(
            FetchThreadState.StateId.SUBSCRIBE_SUCCESS,
            fetchThread.getFetchThreadState()
                .getStateId()
        );
        nettyHttpRdsCdcReaderConnection.fetchMessages(
            dataRequest,
            null
        );
        Assert.assertEquals(
            FetchThreadState.StateId.FETCH_EVENTS_SUCCESS,
            fetchThread.getFetchThreadState()
                .getStateId()
        );
        nettyHttpRdsCdcReaderConnection.close();
    }
}
