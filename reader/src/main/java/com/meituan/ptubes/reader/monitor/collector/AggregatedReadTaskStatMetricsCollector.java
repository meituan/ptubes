package com.meituan.ptubes.reader.monitor.collector;

import com.meituan.ptubes.reader.monitor.vo.ReaderTaskInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AggregatedReadTaskStatMetricsCollector {

    private Map<String, ReaderTaskStatMetricsCollector> readTaskStatMetricsCollectorMap = new ConcurrentHashMap<>();

    public synchronized ReaderTaskStatMetricsCollector registerReaderTaskStatMetricsCollector(String readerTaskName, ReaderTaskStatMetricsCollector collector) {
        readTaskStatMetricsCollectorMap.put(readerTaskName, collector);
        return collector;
    }

    public synchronized void unregisterReaderTaskStatMetricsCollector(String readerTaskName) {
        readTaskStatMetricsCollectorMap.remove(readerTaskName);
    }

    public ReaderTaskStatMetricsCollector getReaderTaskCollector(String readerTaskName) {
        return readTaskStatMetricsCollectorMap.get(readerTaskName);
    }

    public List<ReaderTaskInfo> toReaderTaskInfos() {
        List<ReaderTaskInfo> ans = new ArrayList<>();

        for (ReaderTaskStatMetricsCollector collector : readTaskStatMetricsCollectorMap.values()) {
            ans.add(new ReaderTaskInfo(
                collector.getReaderTaskName(),
                collector.getReaderTaskStartTime(),
                collector.getEventWriteCounter(),
                collector.getEventReadCounter(),
                collector.getLastEventBinlogTime(),
                collector.getLastEventDbToProducerCost(),
                collector.getLastEventProducerToStorageCost(),
                collector.getLastEventStorageToReadChannelCost(),
                collector.getLastHeartBeatTime(),
                collector.getHeartbeatRecieveTimestamp(),
                collector.getLastHeartBeatBinlogInfo().toMap(),
                collector.getStorageMode().name(),
                new ReaderTaskInfo.StorageEngineDetail(
                    collector.getMemStorageMinBinlogInfo()
                        .toMap(),
                    collector.getMemStorageMaxBinlogInfo()
                        .toMap(),
                    collector.getMemStorageEventReadCounter()
                ),
                new ReaderTaskInfo.StorageEngineDetail(
                    collector.getFileStorageMinBinlogInfo()
                        .toMap(),
                    collector.getFileStorageMaxBinlogInfo()
                        .toMap(),
                    collector.getFileStorageEventReadCounter()
                )
            ));
        }

        return ans;
    }

}
