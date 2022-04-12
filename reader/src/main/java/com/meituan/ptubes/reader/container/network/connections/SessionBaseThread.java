package com.meituan.ptubes.reader.container.network.connections;

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

// The thread model should be decoupled from classes such as session as much as possible
public abstract class SessionBaseThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SessionBaseThread.class);

    private final Session session;
    // thread status
    protected boolean isShutdown = false;
    // session-threads synchronize control
    protected final Phaser resetPhaser;
    protected final Phaser shutdownPhaser;
    // external synchronize control
    protected Lock controlLock = new ReentrantLock(true);
    protected Condition shutdownFinishCondition = controlLock.newCondition();

    public SessionBaseThread(
        Session session,
        String threadName,
        Phaser resetPhaser,
        Phaser shutdownPhaser
    ) {
        super(threadName);
        setDaemon(true);
        this.session = session;
        this.resetPhaser = resetPhaser;
        this.shutdownPhaser = shutdownPhaser;
    }

    @Override
    public void run() {
        boolean breakLoop = false;
        boolean resetResult = false;
        String clientId = session.getClientId();
        try {
            while (!breakLoop && !session.hasShutdownRequest()) {
                try {
                    if (session.hasResetRequest()) {
                        // isReset multiple threads may not be consistent, so the function of synchronously waiting for reset is not provided for the time being, ABA problem, and isShutdown will only be converted once
                        // One kind of liberation is to use the object variable for isReset. If the object changes, it means change, but the thread of wait is required to save the isReset object before the trigger operation.
                        try {
                            resetResult = resetRun();
                            resetPhaser.arriveAndAwaitAdvance();
                        } catch (Throwable te) {
                            LOG.error("session {} reset error, break loop", clientId, te);
                            session.closeAsync();
                            resetResult = false;
                        } finally {
                            breakLoop = !resetResult;
//                            notifyResetExit();
                        }
                        continue;
                    }

                    breakLoop = !runOnce();
                } catch (Throwable te) {
                    LOG.error("session {} cause exception, break loop and shutdown", clientId, te);
                    session.closeAsync();
                    breakLoop = true;
                }
            }

            session.closeAsync();
            // Don't throw an exception here, otherwise the shutdown link may be skipped
            try {
                resetPhaser.arriveAndDeregister();
            } catch (Throwable te) {
                LOG.error("session {} deregister from resetPhaser error", clientId, te);
            }

            // isShutdown can only be from false to true and can be synchronized externally
            try {
                shutdownRun();
                shutdownPhaser.arriveAndAwaitAdvance();
            } catch (Throwable te) {
                LOG.error("session {} shutdown error, maybe cause resource leaking", clientId, te);
                shutdownPhaser.arriveAndDeregister();
            } finally {
                notifyShutdownFinish();
            }
        } catch (Throwable te) {
            LOG.error("session {} cause exception", clientId, te);
        } finally {
            try {
                finalRun();
            } catch (Throwable te) {
                LOG.error("session {} final run error", clientId, te);
            }
        }
    }

    
    protected abstract void init() throws Exception;
    protected abstract boolean resetRun() throws Exception;
    protected abstract boolean runOnce() throws Exception;
    @Deprecated
    protected abstract boolean breakLoopRun() throws Exception;
    protected abstract boolean shutdownRun() throws Exception;
    protected abstract void finalRun();

    public void waitForShutdownFinish() throws InterruptedException {
        controlLock.lock();
        try {
            while (!isShutdown) {
                shutdownFinishCondition.await();
            }
        } finally {
            controlLock.unlock();
        }
    }
    public boolean waitForShutdownFinish(long timeout, TimeUnit timeUnit) throws InterruptedException {
        controlLock.lock();
        try {
            while (!isShutdown) {
                return shutdownFinishCondition.await(timeout, timeUnit);
            }
        } finally {
            controlLock.unlock();
        }
        return true;
    }
    public void notifyShutdownFinish() {
        controlLock.lock();
        try {
            isShutdown = true;
            shutdownFinishCondition.signalAll();
        } finally {
            controlLock.unlock();
        }
    }
}
