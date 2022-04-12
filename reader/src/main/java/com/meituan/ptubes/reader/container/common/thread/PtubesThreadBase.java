package com.meituan.ptubes.reader.container.common.thread;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.exception.PtubesException;

public abstract class PtubesThreadBase extends Thread {
	protected final Logger log;

	protected Lock controlLock = new ReentrantLock(true);
	protected Condition shutdownCondition = controlLock.newCondition();
	protected Condition pauseCondition = controlLock.newCondition();
	protected Condition resumeCondition = controlLock.newCondition();
	protected Condition resumeRequestCondition = controlLock.newCondition();
	protected boolean shutdownRequested = false;
	protected boolean shutdown = false;
	protected boolean pauseRequested = false;
	protected boolean paused = false;
	protected boolean resumeRequested = false;

	public static final String MODULE = PtubesThreadBase.class.getName();

	public PtubesThreadBase(String name) {
		super(name);
		setDaemon(true);
		log = LoggerFactory.getLogger(getClass().getName() + "." + name);
	}

	@Override
	public void run() {
		try {
			beforeRun();
			boolean done = false;
			while ((!done) && (!isShutdownRequested())) {
				while (isPauseRequested()) {
					log.info("Pausing !!");
					signalPause();
					try {
						// wait for resume
						awaitUnPauseRequest();
						log.info("Resuming !!");
					} catch (InterruptedException ie) {
					}
					signalResumed();
				}

				done = !runOnce();
			}

			log.info("Shutting down !!");
			doShutdownNotify();
			afterRun();
		} catch (PtubesException ex) {
			log.error("Got error. Stopping !! ", ex);
		}
	}

	/**
	 * This is the method that subclasses are supposed to override.
	 * Currently the method has default implementation.
	 *
	 * to be moved to this framework and runOnce() should be marked abstract.
	 *
	 * @return true if the framework has to continue calling this method again or false to exit the loop and shutdown
	 * @throws PtubesException
	 * 		if encountered any fatal errors that requires shutting down this thread.
	 */
	public boolean runOnce() throws PtubesException {
		return false;
	}

	/**
	 * This is the method that subclasses are supposed to override. Called before
	 * runOnce() is called.
	 * Currently the method has default implementation.
	 *
	 * to be moved to this framework and runOnce() should be marked abstract.
	 *
	 * @throws PtubesException
	 * 		if encountered any fatal errors that requires shutting down this thread.
	 */
	public void beforeRun() throws PtubesException {
	}

	/**
	 * This is the method that subclasses are supposed to override.
	 * Called after shutdown notification happens in {@link run()}
	 * Currently the method has default implementation.
	 *
	 * to be moved to this framework and runOnce() should be marked abstract.
	 *
	 * @throws PtubesException
	 * 		if encountered any fatal errors that requires shutting down this thread.
	 */
	public void afterRun() throws PtubesException {
	}

	public void pauseAsynchronously() {
		log.info("Pause requested");
		controlLock.lock();
		try {
			pauseRequested = true;
		} finally {
			controlLock.unlock();
		}
	}

	public void pause() throws InterruptedException {
		pauseAsynchronously();
		awaitPause();
	}

	public void unpauseAsynchronously() {
		log.info("Resume requested");
		controlLock.lock();
		try {
			resumeRequested = true;
			resumeRequestCondition.signal();

		} finally {
			controlLock.unlock();
		}
	}

	public void unpause() throws InterruptedException {
		unpauseAsynchronously();
		awaitUnPause();
	}

	public void signalPause() {
		controlLock.lock();
		try {
			paused = true;
			pauseCondition.signal();
		} finally {
			controlLock.unlock();
		}
	}

	/**
	 * Notify that this thread has resumed.
	 */
	protected void signalResumed() {
		controlLock.lock();
		try {
			paused = false;
			resumeRequested = true;
			resumeCondition.signal();
		} finally {
			controlLock.unlock();
		}
	}

	protected void shutdownAsynchronously() {
		controlLock.lock();
		try {
			shutdownRequested = true;
		} finally {
			controlLock.unlock();
		}
	}

	public void shutdown() {
		shutdownAsynchronously();
		awaitShutdownUniteruptibly();
	}

	public boolean isPauseRequested() {
		controlLock.lock();
		try {
			return pauseRequested;
		} finally {
			controlLock.unlock();
		}
	}

	public boolean isPaused() {
		controlLock.lock();
		try {
			return paused;
		} finally {
			controlLock.unlock();
		}
	}

	public boolean isUnPauseRequested() {
		controlLock.lock();
		try {
			return resumeRequested;
		} finally {
			controlLock.unlock();
		}
	}

	public boolean isUnPaused() {
		controlLock.lock();
		try {
			return !paused;
		} finally {
			controlLock.unlock();
		}
	}

	public boolean isShutdownRequested() {
		controlLock.lock();
		try {
			return shutdownRequested;
		} finally {
			controlLock.unlock();
		}
	}

	public boolean isShutdown() {
		controlLock.lock();
		try {
			return shutdownRequested;
		} finally {
			controlLock.unlock();
		}
	}

	/** Awaits interruptibly for the thread to pause */
	public void awaitPause() throws InterruptedException {
		log.info("Waiting to be paused");
		controlLock.lock();
		try {
			while (!paused) {
                pauseCondition.await();
            }
			pauseRequested = false;
		} finally {
			controlLock.unlock();
		}
		log.info("Paused: true");
	}

	/** Awaits interruptibly for the thread to unpause */
	public void awaitUnPause() throws InterruptedException {
		log.info("Waiting for resumption");
		controlLock.lock();
		try {
			while (paused) {
                resumeCondition.await();
            }
			resumeRequested = false;
		} finally {
			controlLock.unlock();
		}
		log.info("Resumed: true");
	}

	/** Awaits interruptibly for the thread to be unpause */
	protected void awaitUnPauseRequest() throws InterruptedException {
		log.info("Waiting to be requested for resume");
		controlLock.lock();
		try {
			while (!resumeRequested) {
                resumeRequestCondition.await();
            }
		} finally {
			controlLock.unlock();
		}
		log.info("Resume Requested: true");
	}

	/**
	 * Awaits interruptibly for the thread to pause or until time out.
	 *
	 * @return true if the pause happened and false if there was a time out
	 */
	public boolean awaitPause(long timeout, TimeUnit timeUnit) throws InterruptedException {
		log.info("Waiting for pause with timeout");
		boolean success;
		controlLock.lock();
		try {
			while (!paused) {
				boolean successfulWait = pauseCondition.await(timeout, timeUnit);
				if (log.isDebugEnabled()) {
                    log.debug("Await Condition returned :" + successfulWait);
                }
			}
			success = paused;
		} finally {
			controlLock.unlock();
		}

		log.info("Paused: " + success);
		return success;
	}

	/**
	 * Awaits interruptibly for the thread to resume or until time out.
	 *
	 * @return true if the resume happened and false if there was a time out
	 */
	public boolean awaitUnPause(long timeout, TimeUnit timeUnit) throws InterruptedException {
		log.info("Waiting for resume with timeout");
		boolean success;
		controlLock.lock();
		try {
			while (paused) {
				boolean successfulWait = resumeCondition.await(timeout, timeUnit);
				if (log.isDebugEnabled()) {
                    log.debug("Await Condition returned :" + successfulWait);
                }
			}
			success = !paused;
		} finally {
			controlLock.unlock();
		}

		log.info("UnPaused: " + success);
		return success;
	}

	/** Awaits interruptibly for the thread to shutdown */
	public void awaitShutdown() throws InterruptedException {
		log.info("Waiting for shutdown");
		controlLock.lock();
		try {
			while (!shutdown) {
                shutdownCondition.await();
            }
		} finally {
			controlLock.unlock();
		}
		log.info("Shutdown: true");
	}

	/** Awaits interruptibly for the thread to shutdown */
	public void awaitShutdownUniteruptibly() {
		log.info("Waiting for shutdown uninteruptibly");
		boolean keepOnWaiting = true;
		while (keepOnWaiting) {
			try {
				awaitShutdown();
				keepOnWaiting = false;
			} catch (InterruptedException ie) {
			}
		}
		log.info("Shutdown: true");
	}

	/**
	 * Awaits interruptibly for the thread to shutdown or until time out.
	 *
	 * @return true if the shutdown happened and false if there was a time out
	 */
	public boolean awaitShutdownUninteruptibly(long timeout, TimeUnit timeUnit) {
		log.info("Waiting for shutdown uninteruptibly with timeout");
		boolean success;
		controlLock.lock();
		try {
			long startTime = System.nanoTime();
			long timeoutNanos = timeUnit.toNanos(timeout);
			while (!shutdown) {
				try {
					long elapsed = (System.nanoTime() - startTime);
					if (elapsed >= timeoutNanos) {
                        break;
                    }
					success = shutdownCondition.await(timeoutNanos - elapsed, timeUnit);
				} catch (InterruptedException ie) {
				}
			}
			success = shutdown;
		} finally {
			controlLock.unlock();
		}

		log.info("Shutdown: " + success);
		return success;
	}

	/**
	 * Caller is notifying that it is shutting down
	 */
	protected void doShutdownNotify() {
		controlLock.lock();
		try {
			log.info("Signalling shutdown !!");
			shutdown = true;
			shutdownCondition.signalAll();
		} finally {
			controlLock.unlock();
		}
	}
}
