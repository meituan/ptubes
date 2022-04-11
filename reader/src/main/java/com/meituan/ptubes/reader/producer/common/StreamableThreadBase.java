package com.meituan.ptubes.reader.producer.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.reader.container.common.thread.PtubesThreadBase;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public abstract class StreamableThreadBase<INBOUND_DATA_TYPE, OUTBOUND_DATA_TYPE> extends PtubesThreadBase implements EventListener<INBOUND_DATA_TYPE> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamableThreadBase.class);
    private static final int DEFAULT_QUEUE_SIZE = 128;
    private final String readerTaskName;

    // runtime
    private PtubesThreadExceptionHandler<INBOUND_DATA_TYPE> ptubesThreadExceptionHandler;
    private BlockingQueue<INBOUND_DATA_TYPE> toHandleDataEventQueue;
    private List<INBOUND_DATA_TYPE> toProcessDataEventList;

    // Do not restrict users from throwing exceptions
    abstract protected OUTBOUND_DATA_TYPE processEvent(INBOUND_DATA_TYPE dataEvent) throws Exception;
    abstract protected void prepareToStartup() throws Exception;
    abstract protected void prepareToShutdown() throws Exception;

    public StreamableThreadBase(String name) {
        this(name, DEFAULT_QUEUE_SIZE);
    }

    public StreamableThreadBase(String name, int queueSize) {
        super(name + "-Parser");
        this.readerTaskName = name;
        this.toHandleDataEventQueue = new ArrayBlockingQueue<>(queueSize);
        this.toProcessDataEventList = new ArrayList<>();
    }

    public StreamableThreadBase<INBOUND_DATA_TYPE, OUTBOUND_DATA_TYPE> setPtubesThreadExceptionHandler(
        PtubesThreadExceptionHandler ptubesThreadExceptionHandler) {
        this.ptubesThreadExceptionHandler = ptubesThreadExceptionHandler;
        return this;
    }

    /**
     * Define the initialization information. If there is an exception, you need to inform the outside before running, and the outside will recycle the allocated resources according to the actual situation. Once started, each component is abnormal, similar to the prepare of 2PC
     * @throws Exception
     */
    public void init() throws Exception {

    }

    @Override
    public boolean onEvent(INBOUND_DATA_TYPE dateEvent) throws InterruptedException {
        boolean offered = false;
        do {
            // timeout affects the thread's sensitivity to shutdown signals
            try {
                offered = this.toHandleDataEventQueue.offer(dateEvent, 200, TimeUnit.MILLISECONDS);
                if (offered == false && LOG.isDebugEnabled()) {
                    LOG.debug("task {}: Fail to add event into queue", readerTaskName);
                }
            } catch (InterruptedException e) {
                log.error("task {}: Fail to put binlog event to binlogEventQueue", readerTaskName, e);
            }
        } while (!offered && !isShutdownRequested());
        return offered;
    }

    @Override
    public boolean runOnce() throws PtubesException {
        try {
            int taskNum = toHandleDataEventQueue.drainTo(toProcessDataEventList);
            if (taskNum == 0) {
                INBOUND_DATA_TYPE inboundEvent = toHandleDataEventQueue.poll(200, TimeUnit.MILLISECONDS);
                if (Objects.nonNull(inboundEvent)) {
                    toProcessDataEventList.add(inboundEvent);
                }
            }
            for (INBOUND_DATA_TYPE toProcessDataEvent : toProcessDataEventList) {
                try {
                    OUTBOUND_DATA_TYPE outboundEvent = processEvent(toProcessDataEvent);
                } catch (Throwable te) {
                    log.error("process event error", te);
                    if (Objects.nonNull(ptubesThreadExceptionHandler)) {
                        ptubesThreadExceptionHandler.handleEventException(toProcessDataEvent, te);
                    }
                }
            }
        } catch (InterruptedException ie) {
            // Unified processing of interrupt signals, external interrupts should be notified
            if (Objects.nonNull(ptubesThreadExceptionHandler)) {
                ptubesThreadExceptionHandler.handleInterruptException(ie);
            }
        } finally {
            toProcessDataEventList.clear();
        }
        // continue if you can get here
        return true;
    }

    @Override
    public void beforeRun() throws PtubesException {
        try {
            prepareToStartup();
        } catch (Throwable te) {
            if (Objects.nonNull(ptubesThreadExceptionHandler)) {
                ptubesThreadExceptionHandler.handlePrepareToStartupException(te);
            }
            throw new PtubesException(te);
        }
    }

    @Override
    public void afterRun() throws PtubesException {
        try {
            // Recycle resources as much as possible
            toProcessDataEventList.clear();
            toHandleDataEventQueue.clear();
        } catch (Throwable te) {
            if (Objects.nonNull(ptubesThreadExceptionHandler)) {
                ptubesThreadExceptionHandler.handlePrepareToShutdownException(te);
            }
            throw new PtubesException(te);
        }
    }

    @Override
    public void shutdown() {
        try {
            prepareToShutdown();
        } catch (Throwable te) {
            log.error("shutdown error", te);
        }

        super.shutdown();
    }

}
