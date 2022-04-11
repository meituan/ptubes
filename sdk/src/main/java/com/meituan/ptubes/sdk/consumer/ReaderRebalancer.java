package com.meituan.ptubes.sdk.consumer;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.model.ReaderServerInfo;
import com.meituan.ptubes.sdk.model.ServerInfo;
import com.meituan.ptubes.sdk.netty.NettyHttpRdsCdcReaderConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import com.meituan.ptubes.common.utils.HttpUtil;
import com.meituan.ptubes.common.utils.JacksonUtil;
import com.meituan.ptubes.sdk.config.ReaderConnectionConfig;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;



public class ReaderRebalancer implements IReaderRebalancer {


    protected final Logger log;

    private final String taskName;

    public ReaderRebalancer(String taskName) {
        this.taskName = taskName;
        this.log = LoggerFactory.getLoggerByTask(
            ReaderRebalancer.class,
            taskName
        );
    }

    @Override
    public ServerInfo chooseCandidateServer(
            List<ReaderServerInfo> sgServices,
            ServerInfo connectReader,
            ReaderConnectionConfig readerConnectionConfig
    ) {
        if (CollectionUtils.isEmpty(sgServices)) {
            log.error("sgServices is empty return null");
            return null;
        }

        List<ServerInfo> candidateServers = chooseCandidateServers(
            sgServices,
            readerConnectionConfig
        );

        if (candidateServers == null || candidateServers.isEmpty()) {
            log.warn("Candidate servers is null or empty, return null.");
            return null;
        }

        ServerInfo targetServer = null;
        ServerInfo currentConnectReader = null;

        for (ServerInfo candidateServer : candidateServers) {
            if (candidateServer.equals(connectReader)) {
                currentConnectReader = candidateServer;
                break;
            }
        }

        if (currentConnectReader != null) {
            targetServer = currentConnectReader;
        } else {
            //First disrupt the candidateServers, then put them in the TreeSet, select the last Reader with the lowest load in the TreeSet
            Collections.shuffle(candidateServers);
            TreeSet<ServerInfo> candidateServerSet = new TreeSet<>();
            candidateServerSet.addAll(candidateServers);
            targetServer = candidateServerSet.first();
        }

        log.info(String.format(
            "currentConnectReader: %s, targetServer: %s",
            currentConnectReader,
            targetServer
        ));

        return targetServer;
    }

    protected synchronized List<ServerInfo> chooseCandidateServers(
        List<ReaderServerInfo> sgServices,
        ReaderConnectionConfig readerConnectionConfig
    ) {
        List<ServerInfo> availableServers = chooseAvailableServers(
            sgServices,
            readerConnectionConfig
        );
        List<ServerInfo> candidateServers = new ArrayList<>();

        if (availableServers == null || availableServers.isEmpty()) {
            return candidateServers;
        }

        for (ServerInfo availableServer : availableServers) {
            if (availableServer.getWriterTaskList() != null && !availableServer.getWriterTaskList()
                .contains(this.taskName)) {
                candidateServers.add(availableServer);
            }
        }

        if (!candidateServers.isEmpty()) {
            return candidateServers;
        }

        return availableServers;
    }

    protected synchronized List<ServerInfo> chooseAvailableServers(
        List<ReaderServerInfo> sgServices,
        ReaderConnectionConfig readerConnectionConfig
    ) {

        List<ServerInfo> availableServers = new ArrayList<>();

        for (ReaderServerInfo sgService : sgServices) {
            ServerInfo readerServer = getServerInfo(
                sgService,
                readerConnectionConfig
            );

            if (readerServer != null && readerServer.getServerStatus()
                .equals(ServerInfo.ServerStatus.AVAILABLE)) {
                availableServers.add(readerServer);
                log.info(readerServer.toString());
            } else {
                String errorMessage = String.format(
                    "Unavailable service %s for readerConnectionConfig %s.",
                    sgService.getIp(),
                    readerConnectionConfig.toString()
                );

                log.error(errorMessage);
            }
        }

        log.info(String.format(
            "Task %s candidate server size %s.",
            this.taskName,
            availableServers.size()
        ));

        if (availableServers.isEmpty()) {
            String errorMessage = "no available service for reader task: " +
                readerConnectionConfig.getServiceGroupInfo()
                    .getServiceGroupName();
            log.error(errorMessage);
            return null;
        }

        return availableServers;
    }


    protected ServerInfo getServerInfo(
            ReaderServerInfo sgService,
        ReaderConnectionConfig readerConnectionConfig
    ) {
        String url = new StringBuilder().append("http://")
            .append(sgService.getIp())
            .append(":")
            .append(sgService.getPort())
            .append(NettyHttpRdsCdcReaderConnection.getServerInfoURI())
            .toString();

        String response = HttpUtil.post(
            url,
            JacksonUtil.toJson(
                readerConnectionConfig
            )
        );

        try {
            if (StringUtils.isNotEmpty(response)) {
                ServerInfo serverInfo = JacksonUtil.fromJson(
                    response,
                    ServerInfo.class
                );

                if (serverInfo != null) {
                    serverInfo.setAddress(new InetSocketAddress(
                        InetAddress.getByName(sgService.getIp()),
                        sgService.getPort()
                    ));
                }
                return serverInfo;
            }
        } catch (Exception e) {
            log.error(
                "Request serverInfo fail, IP address " + sgService.getIp(),
                e
            );
        }

        return null;
    }
}
