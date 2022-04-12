/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package com.meituan.ptubes.sdk.consumer;

import com.meituan.ptubes.common.log.Logger;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractActor implements Runnable, IActorMessageQueue, IControllableActor {
    public static final int MAX_QUEUED_MESSAGES = 100;
    public static final long MESSAGE_QUEUE_POLL_TIMEOUT_MS = 100;
    private final Lock controlLock = new ReentrantLock(true);
    private final Condition shutdownCondition = controlLock.newCondition();
    private final Condition newStateCondition = controlLock.newCondition();
    private volatile LifecycleMessage shutdownRequest = null;
    protected LifecycleMessage currentLifecycleState;

    private final MessageQueueFilter pauseFilter = new DefaultPauseFilter();
    private final MessageQueueFilter suspendFilter = new DefaultSuspendFilter();
    private final MessageQueueFilter shutdownFilter = new DefaultShutdownFilter();

    private final String name;
    public Status status;
    protected final Logger log;

    private final Queue<Object> messageQueue = new ArrayDeque<>(MAX_QUEUED_MESSAGES);

    public enum Status {
        INITIALIZING,
        RUNNING,
        PAUSED,
        ERROR_RETRY,
        SUSPENDED_ON_ERROR,
        SHUTDOWN
    }

    public AbstractActor(String name, Logger logger) {
        this.name = name;
        this.currentLifecycleState = null;
        this.status = Status.INITIALIZING;
        log = logger;
    }

    @Override
    public void run() {
        Object nextState = null;

        boolean running = true;

            while ( running && null == shutdownRequest) {
                try {
                nextState = pollNextState();
                if (null == nextState) {
                    running = false;
                    String msg = "AbstractActor quit with next state is null";
                    if (null == shutdownRequest) {
                        log.error(msg);
                    } else {
                        log.warn(msg);
                    }
                } else {
                    running = doExecuteAndChangeState(nextState);
                    if (!running) {
                        String msg = "AbstractActor quit with execute and change state fail";
                        log.error(msg);
                    }
                }
                } catch (Throwable tr) {
                    String msg = "AbstractActor quit with error";
                    log.error(msg);
                    running = false;
                }
            }

        try {
            performShutdown();
        } finally {
            controlLock.lock();
            try {
                status = Status.SHUTDOWN;
                shutdownCondition.signalAll();
            } finally {
                controlLock.unlock();
            }
        }
    }

    private Object pollNextState() {
        Object nextState = null;

        controlLock.lock();
        try {
            while (null == shutdownRequest && messageQueue.isEmpty()) {
                try {
                    newStateCondition.await(MESSAGE_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                }
            }

            if (null == shutdownRequest) {
                nextState = messageQueue.poll();
            }
        } finally {
            controlLock.unlock();
        }

        return nextState;
    }

    @Override
    public void enqueueMessage(Object message) {
        if ( null == message) {
            log.warn("Attempt to queue empty state.");
            return;
        }

        controlLock.lock();

        try {
            if (null == shutdownRequest) {
                switch (this.status) {
                    case SHUTDOWN:
                        log.warn(getName() + ": shutdown: ignoring " + message.toString());
                        break;
                    case PAUSED:
                        log.warn(getName() + ": ignoring message while paused: " + message.toString());
                        break;
                    case SUSPENDED_ON_ERROR:
                        log.warn(getName() + ": ignoring message while suspended_on_error: " + message.toString());
                        break;
                    default: {
                        boolean offerSuccess = messageQueue.offer(message);
                        if (!offerSuccess){
                            log.error(getName() + ": adding a new state failed: " + message.toString() + "; queue.size="
                                          + messageQueue.size());
                        }
                        if (1 == messageQueue.size()){
                            newStateCondition.signalAll();
                        }
                    }
                }
            } else {
                log.warn(getName() + ": shutdown requested: ignoring " + message.toString());
            }
        } finally {
            controlLock.unlock();
        }
    }

    protected void doStart(LifecycleMessage lifecycleMessage) {
        log.info(getName() + ": starting.");
        this.status = Status.RUNNING;
    }

    protected void doPause(LifecycleMessage lifecycleMessage) {
        log.info(getName() + ": pausing.");
        this.status = Status.PAUSED;
        clearQueue(pauseFilter);
    }

    protected void doResume(LifecycleMessage lifecycleMessage) {
        log.info(getName() + ": resuming.");
        this.status = Status.RUNNING;
    }

    protected void doSuspendOnError(LifecycleMessage lifecycleMessage) {
        final Throwable lastError = lifecycleMessage.getLastError();
        if (null != lastError) {
            log.warn(getName() + ": suspending due to " + lastError, lastError);
        } else {
            log.info(getName() + ": suspending.");
        }

        this.status = Status.SUSPENDED_ON_ERROR;
        clearQueue(suspendFilter);
    }

    @Override
    public void pause() {
        log.info(getName() + ": pause requested.");
        LifecycleMessage lifecycleMessage = LifecycleMessage.createPauseMessage();
        enqueueMessage(lifecycleMessage);
    }

    @Override
    public void resume() {
        log.info(getName() + ": resume requested.");
        LifecycleMessage lifecycleMessage = LifecycleMessage.createResumeMessage();
        enqueueMessage(lifecycleMessage);
    }

    @Override
    public void shutdown() {
        log.info(getName() + ": shutdown requested.");
        shutdownRequest = LifecycleMessage.createShutdownMessage();
    }

    public void awaitShutdown() {
        log.info(getName() + ": waiting for shutdown.");
        controlLock.lock();
        try {
            log.info(getName() + ": status at shutdown: " + this.status);
            log.info(getName() + ": queue at shutdown: " + messageQueue);
            while (this.status != Status.SHUTDOWN && this.status != Status.INITIALIZING) {
                shutdownCondition.awaitUninterruptibly();
            }
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * This method is called by the StatMachine thread after it exits out of the main loop in AbstractActorMessageQueue.run()
     */
    private void performShutdown() {
        onShutdown();
        log.info(getName() + " shutdown.");
        clearQueue(shutdownFilter);
    }

    /**
     * This method is called by the StatMachine thread after it exits out of the main loop in AbstractActorMessageQueue.run() and
     * is about to shutdown.
     * After this method returns, the State Machine's status will be set to SHUTDOWN and other waiting threads (for this shutdown) will be signaled.
     * Subclasses must implement this method to cleanup their state for shutdown
     */
    protected abstract void onShutdown();

    public final boolean doExecuteAndChangeState(Object message) {

        boolean success = false;

        try {
            success = executeAndChangeState(message);
        } catch (RuntimeException re) {
            log.error("Stopping because of runtime exception :", re);
        }

        return success;
    }

    protected boolean executeAndChangeState(Object message) {
        boolean success = true;

        if (message instanceof  LifecycleMessage) {
            LifecycleMessage lifecycleMessage = (LifecycleMessage) message;

            switch (lifecycleMessage.getTypeId()) {
                case START:
                    doStart(lifecycleMessage);
                    break;
                case PAUSE:
                    doPause(lifecycleMessage);
                    break;
                case SUSPEND_ON_ERROR:
                    doSuspendOnError(lifecycleMessage);
                    break;
                case RESUME:
                    doResume(lifecycleMessage);
                    break;
                case SHUTDOWN: {
                    log.error("Shutdown message is seen in the queue but not expected : Message :" + lifecycleMessage);
                    success = false;
                    break;
                }
                default: {
                    log.error("Unknown Lifecycle message in RelayPullThread: " + lifecycleMessage.getTypeId());
                    success = false;
                    break;
                }
            }
        }  else {
            log.error("Unknown message of type " + message.getClass().getName() + ": " + message.toString());
            success = false;
        }

        return success;
    }

    protected void clearQueue(MessageQueueFilter messageQueueFilter) {
        controlLock.lock();
        try {
            Iterator<Object> itr = messageQueue.iterator();

            while (itr.hasNext()) {
                if ( !messageQueueFilter.shouldRetain(itr.next())) {
                    itr.remove();
                }
            }
        } finally {
            controlLock.unlock();
        }
    }

    public String getName() {
        return name;
    }

    public boolean isShutdownRequested() {
        return shutdownRequest != null;
    }

    public interface MessageQueueFilter {
        public boolean shouldRetain(Object msg);
    }

    private class DefaultPauseFilter implements MessageQueueFilter {

        @Override
        public boolean shouldRetain(Object msg) {
            return shouldRetainMessageOnPause(msg);
        }
    }

    /**
     * Filter for clearing Message Queue on Suspend_on_Error
     */
    private class DefaultSuspendFilter implements MessageQueueFilter {
        @Override
        public boolean shouldRetain(Object msg) {
            return shouldRetainMessageOnSuspend(msg);
        }
    }

    /**
     * Filter for clearing Message Queue on shutdown
     */
    private class DefaultShutdownFilter implements MessageQueueFilter {
        @Override
        public boolean shouldRetain(Object msg) {
            return shouldRetainMessageOnShutdown(msg);
        }
    }

    protected boolean shouldRetainMessageOnPause(Object msg) {
        if (msg instanceof LifecycleMessage) {
            return true;
        }

        return false;
    }

    protected boolean shouldRetainMessageOnSuspend(Object msg) {
        if (msg instanceof LifecycleMessage) {
            return true;
        }

        return false;
    }

    protected boolean shouldRetainMessageOnShutdown(Object msg) {
        return false;
    }
}
