package com.meituan.ptubes.container.simulator;

import com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.producer.mysqlreplicator.ReplicatorEventProducer;

public class MockedReplicatorEventProducer extends ReplicatorEventProducer {
    private boolean mockIsRunning;
    public MockedReplicatorEventProducer(
            ProducerConfig producerConfig,
            StorageConfig storageConfig,
            ReaderTaskStatMetricsCollector readerTaskCollector
    ) throws PtubesException {
        super(producerConfig, storageConfig, readerTaskCollector);
    }

    public boolean getMockIsRunning() {
        return mockIsRunning;
    }

    public void setMockIsRunning(boolean mockIsRunning) {
        this.mockIsRunning = mockIsRunning;
    }

    @Override
    public synchronized boolean isRunning() {
        return mockIsRunning;
    }

}
