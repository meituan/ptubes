package com.meituan.ptubes.reader.container;

import com.meituan.ptubes.reader.producer.EventProducer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ReaderTask only saves public components. Why not manage the configuration? Considering the ReaderTask configuration format of different types of sources, the storage location may be different, and it is not too abstract, so it is not considered for the time being.
 */
public class ReaderTask {

    private final String readerTaskName;
    private final EventProducer eventProducer;
    private final ExecutorService configListenerExecutor;

    public ReaderTask(String readerTaskName, EventProducer eventProducer) {
        this.readerTaskName = readerTaskName;
        this.eventProducer = eventProducer;
        this.configListenerExecutor = new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(100),
            (ThreadFactory) r -> new Thread(r, "ConfigListener-" + readerTaskName),
            new ThreadPoolExecutor.AbortPolicy());
    }

    public String getReaderTaskName() {
        return readerTaskName;
    }

    public EventProducer getEventProducer() {
        return eventProducer;
    }

    public ExecutorService getConfigListenerExecutor() {
        return configListenerExecutor;
    }
}
