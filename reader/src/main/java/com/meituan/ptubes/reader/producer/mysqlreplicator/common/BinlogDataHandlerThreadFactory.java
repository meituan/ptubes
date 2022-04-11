package com.meituan.ptubes.reader.producer.mysqlreplicator.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.ThreadContext;

public class BinlogDataHandlerThreadFactory implements ThreadFactory {

    private static final AtomicInteger THREAD_ID_GENERATOR = new AtomicInteger(1);

    private final String readerTaskName;

    public BinlogDataHandlerThreadFactory(String readerTaskName) {
        this.readerTaskName = readerTaskName;
    }

    public String getReaderTaskName() {
        return readerTaskName;
    }

    @Override public Thread newThread(Runnable r) {
        return new Thread(new RunnableWrapper(r, readerTaskName), readerTaskName + "-disruptor-" + THREAD_ID_GENERATOR.getAndIncrement());
    }

    public static final class RunnableWrapper implements Runnable {
        private final Runnable r;
        private final String readerTaskName;

        public RunnableWrapper(Runnable r, String readerTaskName) {
            this.r = r;
            this.readerTaskName = readerTaskName;
        }

        public Runnable getR() {
            return r;
        }

        public String getReaderTaskName() {
            return readerTaskName;
        }

        @Override public void run() {
            ThreadContext.put("TASKNAME", readerTaskName);
            r.run();
        }
    }
}
