package com.meituan.ptubes.reader.monitor.collector;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.monitor.vo.ClientSessionInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;

// The System.currentTimeMillis() problem can be solved by using the unified clock util to reduce the precision
public class AggregatedClientSessionStatMetricsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(AggregatedClientSessionStatMetricsCollector.class);

    Map<String/*clientId*/, ClientSessionStatMetricsCollector> clientSessionStatMetricsCollectorMap = new ConcurrentHashMap<>();

    public synchronized ClientSessionStatMetricsCollector registerClientSessionStatMetricsCollector(
        String clientId,
        String writeTaskName,
        String ip,
        int port,
        BinlogInfo initBinlogInfo
    ) {
        long currentTime = System.currentTimeMillis();
        ClientSessionStatMetricsCollector collector = new ClientSessionStatMetricsCollector(initBinlogInfo);
        collector.setWriteTaskName(writeTaskName);
        collector.setIp(ip);
        collector.setPort(port);
        collector.setFirstSubscriptionTime(currentTime);
        collector.setLastSubscriptionTime(currentTime);
        clientSessionStatMetricsCollectorMap.put(clientId, collector);
        return collector;
    }

    public synchronized void unregisterClientSessionStatMetricsCollector(String clientId) {
        if (clientSessionStatMetricsCollectorMap.containsKey(clientId)) {
            clientSessionStatMetricsCollectorMap.remove(clientId);
        }
    }

    public List<String> clientSessionTaskName() {
        return clientSessionStatMetricsCollectorMap.values()
            .stream()
            .map(ClientSessionStatMetricsCollector::getWriteTaskName)
            .collect(Collectors.toList());
    }

    public synchronized List<ClientSessionInfo> toClientSessionInfos() {
        List<ClientSessionInfo> ans = new ArrayList<>();

        for (ClientSessionStatMetricsCollector collector : clientSessionStatMetricsCollectorMap.values()) {
            ans.add(new ClientSessionInfo(collector.getWriteTaskName(),
                                          collector.getIp(),
                                          collector.getPort(),
                                          collector.getInboundTraffic(),
                                          collector.getOutboundTraffic(),
                                          collector.getSubRequestCounter(),
                                          collector.getLastSubRequestCost(),
                                          collector.getSubRequestErrorCounter(),
                                          collector.getLastSubscriptionTime(),
                                          collector.getFirstSubscriptionTime(),
                                          collector.getGetRequestCounter(),
                                          collector.getLastGetRequestCost(),
                                          collector.getGetRequestErrorCounter(),
                                          collector.getLastGetTime(),
                                          Objects.nonNull(collector.getLastGetBinlogInfo()) ? collector.getLastGetBinlogInfo().toMap() : Collections.emptyMap()
            ));
        }

        return ans;
    }

}
