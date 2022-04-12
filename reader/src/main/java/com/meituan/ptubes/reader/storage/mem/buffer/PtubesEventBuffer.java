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
package com.meituan.ptubes.reader.storage.mem.buffer;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.InvalidEventException;
import com.meituan.ptubes.common.exception.KeyTypeNotImplementedException;
import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.PtubesEventV2Factory;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntryFactory;
import com.meituan.ptubes.reader.storage.common.event.ErrorEvent;
import com.meituan.ptubes.reader.storage.common.event.EventFactory;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import com.meituan.ptubes.reader.storage.common.event.InternalEventsListener;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.utils.BufferUtil;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.storage.common.event.EventInternalWritable;
import com.meituan.ptubes.reader.storage.mem.buffer.index.BinlogInfoIndex;
import com.meituan.ptubes.reader.storage.mem.lock.Range;
import com.meituan.ptubes.reader.storage.mem.lock.RangeBasedReaderWriterLock;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;


public class PtubesEventBuffer implements Iterable<EventInternalWritable>, EventBufferAppendable {
	public static final String MODULE = PtubesEventBuffer.class.getName();
	public static final EventFactory EVENT_FACTORY = new PtubesEventV2Factory();
	public static final Logger LOG = LoggerFactory.getLogger(MODULE);
	public static final String MMAP_META_INFO_FILE_NAME = "metaFile";
	public static final String SESSION_PREFIX = "session_";
	public static final String MMAP_META_INFO_SUFFIX = ".info";
	public final Logger log;

	protected static final AtomicLong ITERATORS_COUNTER = new AtomicLong(0);

	private final EventFactory eventFactory = EVENT_FACTORY;
	private final SourceType sourceType;

	private byte eventSerializationVersion = eventFactory.getVersion();
	private boolean scnRegress = false;

	private static int MIN_INITIAL_ITERATORS = 30;

	// Locks and state around locks
	private final ReentrantLock queueLock = new ReentrantLock();
	private final Condition notFull = queueLock.newCondition();
	private final Condition notEmpty = queueLock.newCondition();
	protected final RangeBasedReaderWriterLock rwLockProvider;
	private final AtomicInteger readLocked = new AtomicInteger(0);
	private final String readerTaskName;

	/**
	 * keeps track of the current write position (may not be equal to tail because tail is moved lazily on endEvents call)
	 */
	protected final BufferPosition currentWritePosition;

	/**
	 * An index that keeps track of binlogInfo -> offset mappings
	 */
	protected final BinlogInfoIndex binlogInfoIndex;

	protected final StorageConstant.IndexPolicy indexPolicy;

	/**
	 * A list of ByteBuffers to allow PtubesEventBuffer to grow beyond a single ByteBuffer size limitation (2GB). In the
	 * future, we might want to use this to support dynamically resizing the buffer. Invariants for each buffer: (1) if it
	 * has been completely filled in, then the limit() points right after the last written byte; otherwise, limit() ==
	 * capacity()
	 */
	protected final ByteBuffer[] buffers;

	/**
	 * head and tail of the whole buffer (abstracting away the fact that there are multiple buffers involved) head points
	 * to the first valid (oldest) event in the oldest event window in the buffer tail points to the next writable
	 * location in the buffer
	 *
	 * Initially : head starts off as 0, tail starts off at 0
	 */
	protected final BufferPosition head;
	protected final BufferPosition tail;

	// When head == tail, use isEmpty to distinguish between isEmpty and full buffer
	private boolean isEmpty;

	private boolean isClosed = false;

	/** Allocated memory for the buffer */
	private final long allocatedSize;

	private final HashSet<InternalEventsListener> internalListeners = new HashSet<InternalEventsListener>();
	private File mmapSessionDirectory;
	private File mmapDirectory;
	private String sessionId;

	private final StorageConfig.MemConfig memConfig;

	// Pool of iterators
	protected final Set<WeakReference<BaseEventIterator>> busyIteratorPool = new HashSet<>(MIN_INITIAL_ITERATORS);

	// State to support event window "transactions"
	private EventBufferConstants.WindowState eventState = EventBufferConstants.WindowState.INIT;
	private final BufferPosition eventStartIndex;
	/** last binlogInfo written; the max binlogInfo */
	private volatile BinlogInfo lastWrittenBinlogInfo = null;
	/** the binlogInfo for which we've seen the EOW event */
	private volatile BinlogInfo seenEndOfPeriodBinlogInfo = null;
	/**
	 * first binlogInfo after which stream requests can be served from the buffer See DDS-699 for description
	 */
	private volatile BinlogInfo prevBinlogInfo;
	/** timestamp of first event **/

	protected final BufferPositionParser bufferPositionParser;
	/** timestamp of first data event in buffer; at head **/
	private volatile long timestampOfFirstEvent;
	/** binlogInfo of first event */
	private volatile BinlogInfo minBinlogInfo;

	/** timestamp of latest data event of buffer **/
	private volatile long timestampOfLatestDataEvent = 0;

	/** The last generated session id; we keep track of those to avoid duplicates */
	private static SessionIdGenerator sessionIdGenerator = new SessionIdGenerator();

	// Ref counting for the buffer
	private int refCount = 0;
	private long tsRefCounterUpdate = Long.MAX_VALUE;

	protected static class SessionIdGenerator {
		private long lastSessionIdGenerated = -1;

		synchronized String generateSessionId(String name) {
			// just in case - to guarantee uniqueness
			long sessionId;
			while ((sessionId = System.currentTimeMillis()) <= lastSessionIdGenerated) {
				;
			}
			lastSessionIdGenerated = sessionId;
			return SESSION_PREFIX + name + "_" + lastSessionIdGenerated;
		}

	}

	/**
	 * Iterator over a fixed range of events in the buffer and with no locking. The remove() method is not supported.
	 */
	protected class BaseEventIterator implements Iterator<EventInternalWritable> {
		protected final BufferPosition currentPosition;
		protected final BufferPosition iteratorTail;
		protected EventInternalWritable iteratingEvent; 
		protected String identifier;

		public BaseEventIterator(long head, long tail, String iteratorName) {
			currentPosition = new BufferPosition(bufferPositionParser, buffers);
			currentPosition.setPosition(head);

			iteratorTail = new BufferPosition(bufferPositionParser, buffers);
			iteratorTail.setPosition(tail);
			iteratingEvent = eventFactory.createWritableDbusEvent();
			reset(head, tail, iteratorName);
			trackIterator(this);
		}

		//PUBLIC ITERATOR METHODS
		@Override
		public boolean hasNext() {
			// make sure that currentPosition is wrapped around the buffer limit
			boolean result = false;
			if (currentPosition.init()) {
				result = false;
			} else {
				try {
					currentPosition.sanitize();
					iteratorTail.sanitize();
					if (currentPosition.getPosition() > iteratorTail.getPosition()) {
						LOG.error("unexpected iterator state: this:" + this + " \nbuf: " + PtubesEventBuffer.this);
						throw new PtubesRunTimeException("unexpected iterator state: " + this);
					}
					result = (currentPosition.getPosition() != iteratorTail.getPosition());

					if (LOG.isDebugEnabled()) {
						LOG.debug(
								" - hasNext = " + result + " currentPosition = " + currentPosition + " iteratorTail = "
										+ iteratorTail + "limit = " + buffers[0].limit() + "tail = " + tail);
					}
				} catch (PtubesRunTimeException e) {
					log.error("error in hasNext for iterator: " + this);
					log.error("buffer: " + PtubesEventBuffer.this);
					throw e;
				}
			}
			return result;
		}

		@Override
		public EventInternalWritable next() {
			try {
				return next(false);
			} catch (InvalidEventException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder(getClass().getSimpleName());
			builder.append(": {");
			printInternalState(builder);
			builder.append("}");
			return builder.toString();
		}

		/**
		 * Removes all events that have been consumed so far by the iterator This could in the extreme case clear the buffer
		 */
		@Override
		public void remove() {
			throw new PtubesRunTimeException("not supported");
		}

		//OTHER PUBLIC METHODS
		public void close() {
			untrackIterator(this);
		}

		/**
		 * Copy local state into the passed in iterator. If a new iterator is allocated, it is caller's responsibility to
		 * release it.
		 *
		 * Does not change destination iterator name
		 *
		 * @param destinationIterator
		 * 		: the iterator which will be changed to become a copy of this
		 */
		public BaseEventIterator copy(BaseEventIterator destinationIterator, String iteratorName) {
			if (null == destinationIterator) {
				destinationIterator = acquireLockFreeInternalIterator(currentPosition.getPosition(),
						iteratorTail.getPosition(), iteratorName);
			}
			destinationIterator.reset(currentPosition.getPosition(), iteratorTail.getPosition(),
					destinationIterator.identifier);

			return destinationIterator;
		}

		public String printExtendedStateInfo() {
			String baseState = toString();
			return baseState + "; buffer.head: " + head + "; buffer.tail: " + tail + "; buffer.currentWritePosition: "
					+ currentWritePosition;
		}

		//INTERNAL STATE MANAGEMENT
		@Override
		protected void finalize() throws Throwable {
			close();
			super.finalize();
		}

		/**
		 * Reset the iterator with a new reality w.r.t start and end points
		 */
		protected void reset(long head, long tail, String iteratorName) {
			assert head >= 0 : "name:" + iteratorName;
			assert head <= tail : "head:" + head + "; tail: " + tail + "; name:" + iteratorName;

			identifier = null != iteratorName ? iteratorName :
					getClass().getSimpleName() + ITERATORS_COUNTER.getAndIncrement();

			currentPosition.setPosition(head);
			iteratorTail.setPosition(tail);

			assertPointers();
			assert iteratingEvent != null;
		}

		public EventInternalWritable next(boolean validateEvent) throws InvalidEventException {
			final long oldPos = currentPosition.getPosition();

			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			EventInternalWritable nextEvent = currentEvent();
			if (validateEvent) {
				if (!nextEvent.isValid()) {
					log.error("invalid event in iterator detected:" + this);
					//if the event is corrupted print a reasonable amount of bytes
					int dumpLen = Math.min(100, Math.max(nextEvent.size(), 100));
					log.error("current event bytes:" + hexdumpByteBufferContents(currentPosition.getPosition(),
							dumpLen));
					if (oldPos >= 0) {
						log.error("previous event bytes @ " + bufferPositionParser.toString(oldPos, buffers) + ": "
								+ hexdumpByteBufferContents(oldPos, 100));
					}
					throw new InvalidEventException();
				}
			}

			try {
				currentPosition.incrementOffset(nextEvent.size());
			} catch (PtubesRunTimeException e) {
				if (oldPos >= 0) {
					log.error("previous event bytes @ " + bufferPositionParser.toString(oldPos, buffers) + ": "
							+ hexdumpByteBufferContents(oldPos, 100));
				}
				binlogInfoIndex.printVerboseString();
				throw new InvalidEventException("error in incrementOffset: " + e.getMessage(), e);
			}
			return nextEvent;
		}

		/**
		 * Get the current event pointed to by the iterator
		 */
		protected EventInternalWritable currentEvent() {
			currentPosition.sanitize();
			assert iteratingEvent != null;
			iteratingEvent = (EventInternalWritable) iteratingEvent.reset(buffers[currentPosition.bufferIndex()],
					currentPosition.bufferOffset());
			return iteratingEvent.createCopy();
		}

		/**
		 * Get the current position pointed to by the iterator Package private to allow helper classes to access
		 * currentPosition
		 */
		protected long getCurrentPosition() {
			currentPosition.sanitize();
			return currentPosition.getPosition();
		}

		protected StringBuilder printInternalState(StringBuilder builder) {
			if (null == builder) {
				builder = new StringBuilder();
			}
			builder.append("identifier: ");
			builder.append(identifier);
			builder.append('-');
			builder.append(System.identityHashCode(this));
			builder.append(", currentPosition: ");
			builder.append(currentPosition);
			builder.append(", iteratorTail: ");
			builder.append(iteratorTail);
			assert iteratingEvent != null;
			if (BaseEventIterator.this.hasNext() && iteratingEvent.isValid()) {
				builder.append(", iteratingEvent: ");
				builder.append(iteratingEvent);
			}
			return builder;
		}

		protected void assertPointers() {
			assert (currentPosition.getPosition() >= head.getPosition()) : printExtendedStateInfo();
			assert (iteratorTail.getPosition() <= currentWritePosition.getPosition()) : printExtendedStateInfo();
		}

		// PUBLIC GETTERS
		public String getIdentifier() {
			return identifier;
		}

		public PtubesEventBuffer getEventBuffer() {
			return PtubesEventBuffer.this;
		}
	}

	/**
	 * Iterator over a fixed range of events in the buffer with locking.
	 */
	public class InternalEventIterator extends BaseEventIterator {
		protected RangeBasedReaderWriterLock.LockToken lockToken;

		public InternalEventIterator(long head, long tail, String iteratorName) {
			super(head, tail, iteratorName);
		}

		//OTHER PUBLIC METHODS
		@Override
		public void close() {
			releaseReadLock();
			super.close();
		}

		/**
		 * Copy local state into the passed in iterator. If a new iterator is allocated, it is caller's responsibility to
		 * release it.
		 *
		 * Does not change destination iterator name
		 *
		 * @param destinationIterator
		 * 		: the iterator which will be changed to become a copy of this
		 */
		public InternalEventIterator copy(InternalEventIterator destinationIterator, String iteratorName) {
			if (null == destinationIterator) {
				destinationIterator = acquireInternalIterator(currentPosition.getPosition(),
						iteratorTail.getPosition(), iteratorName);
			} else {
				destinationIterator.reset(currentPosition.getPosition(), iteratorTail.getPosition(),
						destinationIterator.identifier);
			}

			return destinationIterator;
		}

		//INTERNAL STATE MANAGEMENT

		/**
		 * Reset the iterator with a new reality w.r.t start and end points
		 */
		@Override
		protected void reset(long head, long tail, String iteratorName) {
			super.reset(head, tail, iteratorName);
		}

		@Override
		protected StringBuilder printInternalState(StringBuilder builder) {
			builder = super.printInternalState(builder);
			if (null != lockToken) {
				builder.append(", lockToken=");
				builder.append(lockToken);
			}
			return builder;
		}

		// LOCK MANAGEMENT

		/**
		 * re-acquire read lock for the range addressed by the iterator
		 *
		 * @throws TimeoutException
		 * @throws InterruptedException
		 */
		protected synchronized void reacquireReadLock() throws InterruptedException, TimeoutException {
			if (lockToken != null) {
				rwLockProvider.releaseReaderLock(lockToken);
				lockToken = null;
			}

			if (currentPosition.getPosition() >= 0) {
				lockToken = rwLockProvider.acquireReaderLock(currentPosition.getPosition(),
						iteratorTail.getPosition(), bufferPositionParser,
						getIdentifier() + "-" + System.identityHashCode(this));
			}
		}

		protected synchronized void releaseReadLock() {
			if (lockToken != null) {
				rwLockProvider.releaseReaderLock(lockToken);
				lockToken = null;
			}
		}

		protected  boolean isPointersValid() {
			return (currentPosition.getPosition() >= head.getPosition()) && (iteratorTail.getPosition() <= currentWritePosition.getPosition());
		}

		protected  boolean isRangeValid() {
			return isPointersValid() && (null == lockToken || lockToken.getRange().start <= currentPosition
					.getPosition()) && (null == lockToken || lockToken.getRange().end >= iteratorTail
					.getPosition());
		}

		@Override
		protected void assertPointers() {
			super.assertPointers();
			assert (null == lockToken || lockToken.getRange().start <= currentPosition
					.getPosition()) : printExtendedStateInfo();
			assert (null == lockToken || lockToken.getRange().end >= iteratorTail
					.getPosition()) : printExtendedStateInfo();
		}
	}

	/**
	 * An iterator that will automatically release any resources once it goes past its last element.
	 */
	protected class ManagedEventIterator extends InternalEventIterator {

		public ManagedEventIterator(long head, long tail, String iteratorName) {
			super(head, tail, iteratorName);
		}

		public ManagedEventIterator(long head, long tail) {
			this(head, tail, "ManagedEventIterator" + ITERATORS_COUNTER.getAndIncrement());
		}

		@Override
		public boolean hasNext() {
			boolean hasMore = super.hasNext();
			if (!hasMore) {
				close();
			}
			return hasMore;
		}

	}

	/**
	 * Iterator over events in the buffer. Unlike {@link InternalEventIterator}, this class will sync its state with the
	 * underlying buffer and new events added to the buffer will become visible to the iterator.
	 */
	public class PtubesEventIterator extends InternalEventIterator {
		protected PtubesEventIterator(long head, long tail, String iteratorName) {
			super(head, tail, iteratorName);
		}

		/**
		 * Copy local state into the passed in iterator. If a new iterator is allocated, it is caller's responsibility to
		 * release it.
		 *
		 * Does not change destination iterator name
		 *
		 * @param destinationIterator
		 * 		: the iterator which will be changed to become a copy of this
		 */
		public PtubesEventIterator copy(PtubesEventIterator destinationIterator, String iteratorName) {
			if (null == destinationIterator) {
				destinationIterator = new PtubesEventIterator(currentPosition.getPosition(), iteratorTail.getPosition(),
						iteratorName);
			} else {
				super.copy(destinationIterator, iteratorName);
			}

			return destinationIterator;
		}

		/**
		 * Shrinks the iterator tail to the currentPosition
		 */
		public void trim() {
			iteratorTail.copy(currentPosition);
		}

		/**
		 * Synchronizes the state of the iterator with the state of the buffer. In particular, the tail of the iterator may
		 * lag behind the tail of the buffer as it is updated only explicitly using this method.
		 *
		 * @throws TimeoutException
		 * @throws InterruptedException
		 */
		private void copyBufferEndpoints() throws InterruptedException, TimeoutException {
			final boolean debugEnabled = log.isDebugEnabled();
			acquireWriteLock();
			try {
				final long oldPos = currentPosition.getPosition();
				final long oldTail = iteratorTail.getPosition();
				try {
					iteratorTail.copy(tail);

					if (head.getPosition() < 0) {
						currentPosition.setPosition(-1);
					} else if (currentPosition.getPosition() < 0) {
						currentPosition.copy(head);
					}

					if (empty() || currentPosition.getPosition() < head.getPosition()) {
						currentPosition.copy(head);
					}
				} finally {
					if (oldPos != currentPosition.getPosition() || oldTail != iteratorTail.getPosition()) {
						if (debugEnabled) {
							log.debug("refreshing iterator: " + this);
						}

						reacquireReadLock();

						if (debugEnabled) {
							log.debug("done refreshing iterator: " + this);
						}
					}
				}
			} finally {
				releaseWriteLock();
			}
		}

		/**
		 * Allows a reader to wait on the iterator until there is new data to consume or time elapses
		 */
		public boolean await(long time, TimeUnit unit) {
			final boolean isDebug = LOG.isDebugEnabled();
			// wait for notification that there is data to consume
			acquireWriteLock();
			try {
				try {
					copyBufferEndpoints();
					boolean available = hasNext();
					if (!available) {
						if (isDebug) {
							LOG.debug(toString() + ": waiting for notEmpty" + this);
						}

						available = notEmpty.await(time, unit);

						if (isDebug) {
							LOG.debug("notEmpty coming out of await: " + available);
						}

						if (available) {
							copyBufferEndpoints();
						}
					}
					return available;
				} catch (InterruptedException e) {
					LOG.warn(toString() + ": await/refresh interrupted");
					return false;
				} catch (TimeoutException e) {
					log.error(toString() + ": timeout waiting for a lock", e);
					return false;
				}
			} finally {
				releaseWriteLock();
			}
		}

		/**
		 * Allows a reader (of the buffer) to wait on the iterator until there is new data to consume.
		 */
		public void await(boolean absorbInterrupt) throws InterruptedException {
			boolean debugEnabled = log.isDebugEnabled();

			// wait for notification that there is data to consume
			acquireWriteLock();
			try {
				try {
					copyBufferEndpoints();
					while (!hasNext()) {
						if (debugEnabled) {
							log.debug(identifier + ": waiting for notEmpty" + this);
						}

						notEmpty.await();
						copyBufferEndpoints();
						if (debugEnabled) {
							log.debug("Iterator " + this + " coming out of await");
						}
					}
				} catch (InterruptedException e) {
					log.warn(toString() + ": await/refresh interrupted", e);
					if (!absorbInterrupt) {
						throw e;
					}
				} catch (TimeoutException e) {
					throw new PtubesRunTimeException(toString() + ": refresh timed out", e);
				}
			} finally {
				releaseWriteLock();
			}
		}

		/**
		 * Allows a reader (of the buffer) to wait on the iterator until there is new data to consume.
		 * above await()! (with absorbInterrupt = true and new absorbTimeout = true)
		 */
		public void awaitInterruptibly() {
			boolean debugEnabled = log.isDebugEnabled();

			// wait for notification that there is data to consume
			acquireWriteLock();
			try {
				try {
					copyBufferEndpoints();
					final boolean wait = true; // never changed => pointless:  ??
					while (wait && !hasNext()) {
						if (debugEnabled) {
							log.debug(identifier + ": waiting for notEmpty" + this);
						}

						notEmpty.await();
						copyBufferEndpoints();
						if (debugEnabled) {
							log.debug("Iterator " + this + " coming out of await");
						}
					}
				} catch (InterruptedException e) {
					log.warn(toString() + ": lock wait/refresh interrupted", e);
				} catch (TimeoutException e) {
					log.error(toString() + ": refresh timed out", e);
				}
			} finally {
				releaseWriteLock();
			}

		}

		@Override
		public boolean hasNext() {
			boolean result = super.hasNext();
			if (!result) {
				//looks like we have reached the end -- give one more try in case iteratorTail was not
				//up-to-date
				try {
					copyBufferEndpoints();
				} catch (InterruptedException e) {
					log.warn(toString() + ": refresh interrupted");
					return false;
				} catch (TimeoutException e) {
					log.error(toString() + ": refresh timed out");
					return false;
				}
				result = super.hasNext();
			}

			return result;
		}

		@Override
		public EventInternalWritable next() {
			EventInternalWritable eventInternalWritable = readNextEvent();
			try {
				if (eventInternalWritable.getEventType().equals(EventType.NO_MORE_EVENT) && hasNext()) {
					return readNextEvent();
				} else {
					return eventInternalWritable;
				}
			} catch (Exception e) {
				log.error("Get next event error", e);
				return ErrorEvent.EXCEPTION_EVENT;
			}
		}

		private EventInternalWritable readNextEvent() {
			if (!isPointersValid()) {
				return ErrorEvent.NOT_IN_BUFFER;
			}
			try {
				reacquireReadLock();
			} catch (Exception e) {
				log.error("reacquireReadLock error", e);
				return ErrorEvent.EXCEPTION_EVENT;
			}

			try {
				if (!isRangeValid()) {
					return ErrorEvent.NOT_IN_BUFFER;
				}
				if (super.hasNext()) {
					return super.next();
				} else {
					return ErrorEvent.NO_MORE_EVENT;
				}
			} catch (Exception e) {
				log.error("Get next event error", e);
				return ErrorEvent.EXCEPTION_EVENT;
			} finally {
				releaseReadLock();
			}
		}

		/**
		 * Removes all events that have been consumed so far by the iterator This could in the extreme case clear the buffer
		 */
		@Override
		public void remove() {
			boolean debugEnabled = LOG.isDebugEnabled();

			if (debugEnabled) {
				LOG.debug(
						"Iterator " + identifier + " hasNext = " + hasNext() + " being asked to remove stuff" + this);
			}

			rwLockProvider.shiftReaderLockStart(lockToken, currentPosition.getPosition(), bufferPositionParser);
			acquireWriteLock();

			try {
				if (isClosed()) {
					LOG.warn("canceling remove operation on iterator because the buffer is closed. it=" + this);
					throw new PtubesRunTimeException(toString() + " remove canceled.");
				}
				copyBufferEndpoints();

				long newHead = currentPosition.getPosition();

				//we need to fetch the binlogInfo for the new head to pass to BinlogInfoIndex.moveHead()
				
				BinlogInfo newBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
				long newTs = -1;
				if (0 <= newHead && newHead < tail.getPosition()) {
					PtubesEvent e = currentEvent();
					assert e.isValid();
					newBinlogInfo = e.getBinlogInfo();
					newTs = e.getTimestampInNS();
				}
				moveHead(newHead, newBinlogInfo, newTs, debugEnabled);
			} catch (InterruptedException e) {
				log.error("buffer locks: " + rwLockProvider);
				throw new PtubesRunTimeException(toString() + ": refresh interrupted", e);
			} catch (TimeoutException e) {
				log.error("remove timeout for iterator " + this + " in buffer " + PtubesEventBuffer.this, e);
				throw new PtubesRunTimeException(toString() + ": refresh timed out", e);
			} catch (RuntimeException e) {
				log.error("error removing events for iterator " + this + ":" + e.getMessage());
				log.error("buffer:" + PtubesEventBuffer.this);
				throw e;
			} catch (AssertionError e) {
				log.error("error removing events for iterator " + this + ":" + e.getMessage());
				log.error("buffer:" + PtubesEventBuffer.this);
				log.error(hexdumpByteBufferContents(currentPosition.getPosition(), 200));
				throw e;
			} finally {
				releaseWriteLock();
			}
		}

		public boolean equivalent(PtubesEventIterator lastSuccessfulIterator) {
			return (lastSuccessfulIterator != null) && lastSuccessfulIterator.currentPosition.equals(currentPosition);
		}

	}

	protected boolean isClosed() {
		if (!queueLock.isLocked()) {
			throw new RuntimeException("checking if buffer is closed should be done under queueLock");
		}

		return isClosed;
	}

	protected void setClosed() throws PtubesException {
		acquireWriteLock();
		try {
			if (isClosed) {
				throw new PtubesException("closing already closed buffer");
			}
			isClosed = true;
		} finally {
			releaseWriteLock();
		}
	}

	public String getReaderTaskName() {
		return readerTaskName;
	}

	public synchronized void increaseRefCounter() {
		refCount++;
		tsRefCounterUpdate = System.currentTimeMillis();
	}

	public synchronized void decreaseRefCounter() {
		refCount--;
		tsRefCounterUpdate = System.currentTimeMillis();
	}

	public synchronized boolean shouldBeRemoved(boolean now) {
		if (refCount > 0) {
			return false;
		}

		if (now) {
			return true;
		}

		return (System.currentTimeMillis() - tsRefCounterUpdate) > memConfig.getBufferRemoveWaitPeriodSec() * 1000;
	}

	public synchronized int getRefCount() {
		return refCount;
	}

	/**
	 * Clears the buffer. Should be only called by consumers who are using the buffer like a "producer - consumer" queue.
	 */
	public void clear() {
		clearAndStart(false, BinlogInfoFactory.newDefaultBinlogInfo(sourceType));
	}

	private void clearAndStart(boolean start, BinlogInfo prevBinlogInfo) {
		acquireWriteLock();

		try {
			if (isClosed()) {
				LOG.warn("canceling clearAndStart because the buffer is being closed");
				return;
			}
			lockFreeClear();
			binlogInfoIndex.clear();
			if (start) {
				this.start(prevBinlogInfo);
			}
			isEmpty = true;
		} finally {
			releaseWriteLock();
		}
	}

	public void reset(BinlogInfo binlogInfo) {
		clearAndStart(true, binlogInfo);
	}

	public long getTimestampOfFirstEvent() {
		return timestampOfFirstEvent;
	}

	/**
	 * Clears the buffer, assumes that requisite locks have been obtained outside this method
	 */
	private void lockFreeClear() {
		binlogInfoIndex.clear();
		head.setPosition(0L);
		tail.setPosition(0L);
		currentWritePosition.setPosition(0L);
		prevBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
		isEmpty = true;
		lastWrittenBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
		timestampOfFirstEvent = 0;
		
		// what happens to the iterators that might be iterating over this buffer?
		// should we call a notifyClear() on them?
		for (ByteBuffer buf : buffers) {
			buf.clear();
		}
		notFull.signalAll();
		//		notifyIterators(head, tail);
	}

	public PtubesEventBuffer(StorageConfig.MemConfig config, String readerTaskName,
							 StorageConstant.IndexPolicy indexPolicy, SourceType sourceType) {
		
		log = StringUtils.isBlank(readerTaskName) ? LOG : LoggerFactory.getLogger(MODULE + "." + readerTaskName);

		log.info("PtubesEventBuffer starting up with " + config.toString());
		log.info("eventFactory: " + eventFactory.getVersion());

		memConfig = config;
		this.indexPolicy = indexPolicy;
		this.sourceType = sourceType;
		List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
		isEmpty = true;
		lastWrittenBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
		prevBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
		timestampOfFirstEvent = 0;
		timestampOfLatestDataEvent = 0;
		this.readerTaskName = StringUtils.isBlank(readerTaskName) ? "default" : readerTaskName;

		// file to read meta info for saved buffers (if any), MetaInfo constructor doesn't create/open any actual file
		PtubesEventBufferMetaInfo mi = null;

		File mmapDirectory = new File(getMmapDir());
		if (config.getAllocationPolicy().equals(EventBufferConstants.AllocationPolicy.MMAPPED_MEMORY) && !mmapDirectory
				.exists()) {
			if (!mmapDirectory.mkdirs()) {
				throw new RuntimeException("Invalid Config Value: Cannot create mmapDirectory: " + this.mmapDirectory);
			}

			if (mmapDirectory.exists() && !mmapDirectory.canWrite()) {
				throw new RuntimeException("Invalid Config Value: Cannot write to mmapDirectory: " + this.mmapDirectory);
			}
		}

		// in case of MMAPED memory - see if there is a meta file, and if there is - read from it
		if (config.getAllocationPolicy() == EventBufferConstants.AllocationPolicy.MMAPPED_MEMORY) {
			sessionId = sessionIdGenerator.generateSessionId(this.readerTaskName); // new session
			File metaInfoFile = new File(getMmapDir(), metaFileName());
			if (config.isRestoreMMappedBuffers()) {
				if (!metaInfoFile.exists()) {
					log.warn("restoreBuffers flag is specified, but the file " + metaInfoFile + " doesn't exist");
				} else if ((System.currentTimeMillis() - metaInfoFile.lastModified())
						> config.getBufferRemoveWaitPeriodSec() * 1000) {
					log.warn("restoreBuffers flag is specified, but the file " + metaInfoFile + " is older than "
							+ config.getBufferRemoveWaitPeriodSec() + " secs");
				} else {
					try {
						mi = new PtubesEventBufferMetaInfo(metaInfoFile);
						mi.loadMetaInfo();
						if (mi.isValid()) {
							sessionId = mi
									.getSessionId(); // figure out what session directory to use for the content of the buffers
							LOG.info("found file " + mi.toString() + "; will reuse session = " + sessionId);

							validateMetaData(config.getMaxEventSizeInByte(), mi);
						} else {
							LOG.warn("cannot restore from file " + metaInfoFile);
						}

					} catch (PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException e) {
						throw new RuntimeException(e);
					}
				}
			}

			// init the directories.
			mmapSessionDirectory = new File(mmapDirectory, sessionId);
			this.mmapDirectory = mmapDirectory;

			if (!mmapSessionDirectory.exists() && !mmapSessionDirectory.mkdirs()) {
				throw new RuntimeException("Could not create directory " + mmapSessionDirectory.getAbsolutePath());
			} else {
				log.info("MMapsessiondir = " + mmapSessionDirectory.getAbsolutePath());
			}

			if (!config.isRestoreMMappedBuffers()) {
				log.info("restoreBuffers is false => will delete mmap session directory " + mmapSessionDirectory
							 + " on exit");
				mmapSessionDirectory.deleteOnExit();
			}
		}

		log.debug("Will allocate a total of " + config.getMaxSizeInByte() + " bytes");
		long allocatedSize = 0;
		while (allocatedSize < config.getMaxSizeInByte()) {
			int nextSize = (int) Math.min(config.getMaxIndividualBufferSizeInByte(),
					(config.getMaxSizeInByte() - allocatedSize));
			if (log.isDebugEnabled()) {
				log.debug("Will allocate a buffer of size " + nextSize + " bytes with allocationPolicy = " + config
						.getAllocationPolicy().toString());
			}

			ByteBuffer buffer;
			if (EventBufferConstants.AllocationPolicy.MMAPPED_MEMORY.equals(config.getAllocationPolicy())) {
				buffer = allocateByteBuffer(nextSize,
											ByteOrder.BIG_ENDIAN,
											config.getAllocationPolicy(),
											config.isRestoreMMappedBuffers(),
											mmapSessionDirectory,
											new File(
												mmapSessionDirectory,
												"writeBuffer_" + buffers.size()
											)
				);
			} else {
				buffer = allocateByteBuffer(nextSize,
											ByteOrder.BIG_ENDIAN,
											config.getAllocationPolicy(),
											config.isRestoreMMappedBuffers(),
											null,
											null
				);
			}

			buffers.add(buffer);
			allocatedSize += nextSize;
		}
		LOG.info("Allocated a total of " + allocatedSize + " bytes into " + buffers.size() + " buffers");
		this.allocatedSize = allocatedSize;

		this.buffers = new ByteBuffer[buffers.size()];
		buffers.toArray(this.buffers);
		if (mi != null && mi.isValid()) {
			try {
				setAndValidateMMappedBuffers(mi);
			} catch (PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException e) {
				throw new RuntimeException(e);
			}
		}

		if (config.getReadBufferSizeInByte() <= getMaxReadBufferCapacity()) {
			config.setReadBufferSizeInByte(config.getReadBufferSizeInByte());
		} else {
			config.setReadBufferSizeInByte(getMaxReadBufferCapacity());
			log.warn(String.format(
					"Initial event staging buffer size %d > than max possible %d event size; " + "resetting to %d ",
					config.getReadBufferSizeInByte(), getMaxReadBufferCapacity(), getMaxReadBufferCapacity()));
		}
		if (0 >= config.getReadBufferSizeInByte()) {
			throw new PtubesRunTimeException(
					"invalid initial event staging buffer size: " + config.getReadBufferSizeInByte());
		}

		bufferPositionParser = new BufferPositionParser(
				(int) (Math.min(config.getMaxIndividualBufferSizeInByte(), config.getMaxSizeInByte())), buffers.size());

		binlogInfoIndex = new BinlogInfoIndex(config.getMaxIndexSizeInByte(), config.getMaxSizeInByte(),
				config.getMaxIndividualBufferSizeInByte(), bufferPositionParser, config.getAllocationPolicy(),
				config.isRestoreMMappedBuffers(), mmapSessionDirectory, config.getAssertLevel(),
				config.isEnableIndex(), ByteOrder.BIG_ENDIAN, this.indexPolicy, this.sourceType);

		head = new BufferPosition(bufferPositionParser, this.buffers);
		tail = new BufferPosition(bufferPositionParser, this.buffers);
		currentWritePosition = new BufferPosition(bufferPositionParser, this.buffers);
		eventStartIndex = new BufferPosition(bufferPositionParser, this.buffers);

		rwLockProvider = new RangeBasedReaderWriterLock();

		if (mi != null && mi.isValid()) {
			try {
				initBuffersWithMetaInfo(mi); // init some of the PtubesEvent Buffer fields from MetaFile if available
				if (config.isRestoreMMappedBuffersValidateEvents()) {
					validateEventsInBuffer();
				}
			} catch (PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException e) {
				throw new RuntimeException(e);
			}
		} else {
			clear(); // clears all stuff
			resetWindowState(); // reset if new buffers only
		}

		// invalidate metainfo to avoid accidental loading of old data
		File metaInfo = new File(this.mmapDirectory, metaFileName());
		if (metaInfo.exists()) {
			File renameTo = new File(metaInfo.getAbsoluteFile() + "." + System.currentTimeMillis());
			// Attention: The parent directory is the same, renameTo is valid
			if (metaInfo.renameTo(renameTo)) {
				log.warn("existing metaInfoFile " + metaInfo + " found. moving it to " + renameTo);
			} else {
				log.error("failed to move existing metaInfoFile " + metaInfo + " to " + renameTo
						+ ". This may cause buffer to load this file if it gets restarted!");
			}
		}
		if (config.isEnableIndex() && binlogInfoIndex.isEmpty()) {
			binlogInfoIndex.setUpdateOnNext(true);
		}
		acquireWriteLock();
		try {
			updateFirstEventMetadata();
		}finally {
			releaseWriteLock();
		}

	}

	String metaFileName() {
		return MMAP_META_INFO_FILE_NAME + "." + readerTaskName;
	}

	String getMmapDir() {
		String dir = ContainerConstants.BASE_DIR + "/" + readerTaskName + "/" + "mmapDir";
		File file = new File(dir);
		if (!file.exists()) {
			file.mkdirs();
		}
		return dir;
	}

	/**
	 * go over all the ByteBuffers and validate them
	 *
	 * @throws PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException
	 */
	private void setAndValidateMMappedBuffers(PtubesEventBufferMetaInfo mi)
			throws PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException {
		// set buffer info - pos and limit
		PtubesEventBufferMetaInfo.BufferInfo[] bufsInfo = null;
		bufsInfo = mi.getBuffersInfo();

		int i = 0;
		for (ByteBuffer buffer : buffers) {
			PtubesEventBufferMetaInfo.BufferInfo bi = bufsInfo[i];

			buffer.position(bi.getPos());
			buffer.limit(bi.getLimit());

			// validate
			if (buffer.position() > buffer.limit() || buffer.limit() > buffer.capacity() || buffer.capacity() != bi
					.getCapacity()) {
				String msg =
						"ByteBuffers don't match: i=" + i + "; pos=" + buffer.position() + "; limit=" + buffer.limit()
								+ "; capacity=" + buffer.capacity() + "; miCapacity=" + bi.getCapacity();
				throw new PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException(mi, msg);
			}
			i++;
		}
		log.info("successfully validated all " + i + " mmapped buffers");
	}

	/**
	 * go thru all the events in the buffer and see that they are valid also compare the event at the end of the buffer
	 * with _lastWrittenSeq
	 *
	 * @throws PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException
	 */
	public void validateEventsInBuffer() throws PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException {
		// go over all the events and validate them

		PtubesEventIterator eventIterator = acquireIterator(head.getPosition(), tail.getPosition(), "validateEventsIterator");

		PtubesEvent e = null;
		int num = 0;
		boolean first = true;
		BinlogInfo firstBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
		long start = System.currentTimeMillis();
		try {
			while (eventIterator.hasNext()) {
				e = eventIterator.next();
				num++;
				if (e.isValid()) {
					if (first) {
						firstBinlogInfo = e.getBinlogInfo();
						first = false;
					}
				} else {
					LOG.error("event " + e + " is not valid");
					throw new PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException(
							"Buffer validation failed. There are some invalid events");
				}
			}
			long time = System.currentTimeMillis() - start;
			LOG.info("scanned " + num + " events in " + time + " msec. event at the end of the buffer: " + e);
			LOG.info("firstBinlogInfo = " + firstBinlogInfo + "; lastWrittenBinlogInfo = " + lastWrittenBinlogInfo
					+ "; minBinlogInfo = " + getMinBinlogInfo());
			if (e != null) {
				BinlogInfo binlogInfo = e.getBinlogInfo();
				if (lastWrittenBinlogInfo.equals(binlogInfo)) {
					throw new PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException(
							"Buffer validation failed. e.binlogInfo=" + binlogInfo + " and lastWrittenBinlogInfo="
									+ lastWrittenBinlogInfo);
				}

			}
		} finally {
			releaseIterator(eventIterator);
		}
	}

	/**
	 * compare and match data between the metaFile and passed in in the constructor
	 *
	 * @param mi
	 * @throws PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException
	 */
	private void validateMetaData(long maxTotalEventBufferSize, PtubesEventBufferMetaInfo mi)
			throws PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException {
		// figure out number of buffers we are going to allocate
		int maxBufferSize = memConfig.getMaxIndividualBufferSizeInByte();
		long numBuffs = maxTotalEventBufferSize / maxBufferSize;
		if (maxTotalEventBufferSize % maxBufferSize > 0) {
			numBuffs++; // calculate number of ByteBuffers we will have
		}
		long miNumBuffs = mi.getLong(PtubesEventBufferMetaInfo.NUM_BYTE_BUFFER);
		if (miNumBuffs != numBuffs) {
			throw new PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException(mi,
					"Invalid number of ByteBuffers in meta file:" + miNumBuffs + "(expected =" + numBuffs + ")");
		}
		// individual buffer size
		long miBufSize = mi.getLong(PtubesEventBufferMetaInfo.MAX_BUFFER_SIZE);
		if (miBufSize != maxBufferSize) {
			throw new PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException(mi,
					"Invalid maxBufferSize in meta file:" + miBufSize + "(expected =" + maxBufferSize + ")");
		}

		// allocatedSize - validate against real buffers
		long allocatedSize = mi.getLong(PtubesEventBufferMetaInfo.ALLOCATED_SIZE);
		if (maxTotalEventBufferSize != allocatedSize) {
			throw new PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException(mi,
					"Invalid maxEventBufferSize in meta file:" + allocatedSize + "(expected =" + maxTotalEventBufferSize
							+ ")");
		}

	}

	public static ByteBuffer allocateDirectByteBuffer(
		int size,
		ByteOrder byteOrder
	) {
		// When allocateDirect will execute gc asynchronously, it is possible that gc has not been executed yet, resulting in memory application failure, so increase retry
		for (int i = 0; i < 5; i++) {
			try {
				return ByteBuffer.allocateDirect(size)
					.order(byteOrder);
			} catch (OutOfMemoryError error) {
				if (i < 4) {
					LOG.warn(
						"Allocate directByteBuffer error, will retry, retryIndex: {}",
						i,
						error
					);
					try {
						Thread.sleep(200);
					} catch (InterruptedException e) {
						LOG.info("Allocate directByteBuffer sleep interrupted");
					}
				} else {
					throw new PtubesRunTimeException("Allocate directByteBuffer error", error);
				}
			}
		}
		throw new PtubesRunTimeException("Allocate directByteBuffer error");
	}

	public static ByteBuffer allocateByteBuffer(int size, ByteOrder byteOrder,
			EventBufferConstants.AllocationPolicy allocationPolicy, boolean restoreBuffers, File mmapSessionDir,
			File mmapFile) {
		ByteBuffer buffer = null;

		switch (allocationPolicy) {
		case HEAP_MEMORY:
			buffer = ByteBuffer.allocate(size).order(byteOrder);
			break;
		case DIRECT_MEMORY:
			buffer = allocateDirectByteBuffer(size, byteOrder);
			break;
		case MMAPPED_MEMORY:
		default:
			// expect that dirs are already created and initialized
			if (!mmapSessionDir.exists()) {
				throw new RuntimeException(mmapSessionDir.getAbsolutePath() + " doesn't exist");
			}

			if (restoreBuffers) {
				if (!mmapFile.exists()) {
					LOG.warn("restoreBuffers is true, but file " + mmapFile + " doesn't exist");
				} else {
					LOG.info("restoring buffer from " + mmapFile);
				}
			} else {
				if (mmapFile.exists()) {
					// this path should never happen (only if the generated session ID accidentally matches a previous one)
					LOG.info("restoreBuffers is false; deleting existing mmap file " + mmapFile);
					if (!mmapFile.delete()) {
						throw new RuntimeException("deletion of file failed: " + mmapFile.getAbsolutePath());
					}
				}
				LOG.info("restoreBuffers is false => will delete new mmap file " + mmapFile + " on exit");
				mmapFile.deleteOnExit(); // in case we don't need files later.
			}

			FileChannel rwChannel = null;
			try {
				rwChannel = new RandomAccessFile(mmapFile, "rw").getChannel();
				buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, size).order(byteOrder);
			} catch (FileNotFoundException e) {
				throw new RuntimeException(
						"[should never happen!] can't find mmap file/dir " + mmapFile.getAbsolutePath(), e);
			} catch (IOException e) {
				throw new RuntimeException("unable to initialize mmap file " + mmapFile, e);
			} finally {
				try {
					if (null != rwChannel) {
						rwChannel.close();
					}
				} catch (IOException e) {
					throw new RuntimeException("unable to close mmap file " + mmapFile, e);
				}
			}
		}
		return buffer;
	}

	RangeBasedReaderWriterLock getRwLockProvider() {
		return rwLockProvider;
	}

	/** Set binlogInfo immediately preceding the minBinlogInfo; */
	public void setPrevBinlogInfo(BinlogInfo binlogInfo) {
		if (log.isDebugEnabled()) {
			log.info("setting prevBinlogInfo to: " + prevBinlogInfo);
		}
		prevBinlogInfo = binlogInfo;
	}

	/**
	 * Get the windowBinlogInfo for the oldest event window in the buffer
	 */
	@Override
	public BinlogInfo getMinBinlogInfo() {
		return minBinlogInfo;
	}

	public void resetWindowState() {
		eventState = EventBufferConstants.WindowState.INIT;
		eventStartIndex.setPosition(-1);
	}

	@Override
	public void start(BinlogInfo binlogInfo) {
		assert ((eventState == EventBufferConstants.WindowState.INIT) || (eventState
				== EventBufferConstants.WindowState.ENDED));
		/*!binlogInfo.isGreaterThan(lastWrittenBinlogInfo, indexPolicy)*/
		if (tail.getPosition() > 0L && !needStartEvent(binlogInfo)) {
			return;
		}
		// When the service is just started, write an isEmpty transaction,
		startEvents();
		appendEvent(ChangeEntryFactory.createSentinelEntry(binlogInfo));
		this.setPrevBinlogInfo(binlogInfo);
	}

	private boolean needStartEvent(BinlogInfo eventBinlogInfo) {
		switch (sourceType) {
			case MySQL:
				return eventBinlogInfo.isGreaterThan(lastWrittenBinlogInfo, indexPolicy);
			default:
				throw new UnsupportedOperationException("Unsupported binlog comparison in source type " + sourceType.name());
		}
	}

	@Override
	public void startEvents() {
		assert ((eventState == EventBufferConstants.WindowState.INIT) || (eventState
				== EventBufferConstants.WindowState.ENDED));
		acquireWriteLock();
		try {
			if (isClosed()) {
				throw new PtubesRunTimeException("attempting startEvents for a closed buffer");
			}
			resetWindowState();
			eventState = EventBufferConstants.WindowState.STARTED;
			// set currentWritePosition to tail.
			// This allows us to silently rollback any writes we did past the tail but never called endEvents() on.
			long tailPosition = tail.getPosition();
			currentWritePosition.setPosition(((tailPosition > 0) ? tailPosition : 0));
		} finally {
			releaseWriteLock();
		}
	}

	@Override
	public int appendEvent(ChangeEntry changeEntry) {
		boolean isDebugEnabled = LOG.isDebugEnabled();
		int bytesWritten = -1;
		acquireWriteLock();
		try {
			assert ((eventState == EventBufferConstants.WindowState.STARTED) || (eventState
					== EventBufferConstants.WindowState.EVENTS_ADDED));
			try {
				binlogInfoIndex.assertHeadPosition(head.getRealPosition());
				bufferPositionParser.assertSpan(head.getPosition(), currentWritePosition.getPosition(),
						isDebugEnabled);
			} catch (RuntimeException re) {
				LOG.error("Got runtime Exception :", re);
				LOG.error("Event Buffer is :" + toString());
				throw re;
			}

			if (isClosed()) {
				throw new PtubesRunTimeException("refusing to append event, because the buffer is closed");
			}

			final int expNumBytesWritten = EventFactory.computeEventLength(changeEntry);
			prepareForAppend(expNumBytesWritten);

			if (eventState == EventBufferConstants.WindowState.STARTED) {
				//We set eventStartIndex here because currentWritePosition is not finalized before
				//the call to prepareForAppend
				eventStartIndex.copy(currentWritePosition);
			}

			if (isDebugEnabled) {
				LOG.debug("serializingEvent at position " + currentWritePosition.toString());
				LOG.debug("PhysicalPartition passed from the buffer = "
						+ readerTaskName);
			}

			byte[] serializedChangeEntry = changeEntry.getSerializedChangeEntry();
			ByteBuffer serializationBuffer = buffers[currentWritePosition.bufferIndex()];
			int startPosition = serializationBuffer.position();
			serializationBuffer.put(serializedChangeEntry);
			int stopPosition = serializationBuffer.position();
			bytesWritten = stopPosition - startPosition;

			//prepareForAppend makes decision to move Head depending upon expNumBytesWritten
			if (bytesWritten != expNumBytesWritten) {
				String msg = "Actual Bytes Written was :" + bytesWritten + ", Expected to Write :" + expNumBytesWritten;
				LOG.error(msg);
				LOG.error("Event Buffer is :" + toString());
				throw new PtubesRunTimeException(msg);
			}

			final long newWritePos = bufferPositionParser.incrementOffset(currentWritePosition.getPosition(),
					bytesWritten, buffers);
			moveCurrentWritePosition(newWritePos);

			eventState = EventBufferConstants.WindowState.EVENTS_ADDED;
			timestampOfLatestDataEvent = Math.max(timestampOfLatestDataEvent, changeEntry.getBinlogInfo().getTs());
		} catch (Exception ex) {
			log.error("Append event error", ex);
			throw new PtubesRunTimeException(ex);
		} finally {
			releaseWriteLock();
		}

		endEvents(changeEntry.getBinlogInfo());
		return bytesWritten;
	}

	/**
	 * Sets up the buffer state to prepare for appending an event. This includes a) moving the head far enough so that the
	 * new event does not overwrite it. - this also implies moving the head for the BinlogInfoIndex to keep it in lock-step with
	 * the buffer b) moving the currentWritePosition to the correct location so that the entire event will fit into the
	 * selected ByteBuffer
	 *
	 * @param eventSize
	 * 		has the size of the event that will be appended.
	 * @throws KeyTypeNotImplementedException
	 */
	private void prepareForAppend(final int eventSize) throws KeyTypeNotImplementedException {
		boolean isDebugEnabled = LOG.isDebugEnabled();

		acquireWriteLock();
		try {
			ByteBuffer buffer = buffers[currentWritePosition.bufferIndex()];

			//try to find a free ByteBuffer with enough space to fit the event
			//we will make at most three attempts: 1) current, possibly half-full, ByteBuffer
			//2) next, possibly last and smaller, ByteBuffer 3) makes sure at least one max capacity
			//ByteBuffer is check
			//when checking for available space at the end of a ByteBuffer always leave one free byte
			//to distinguish between a finalized ByteBuffer (limit <= capacity - 1) and a ByteBuffer
			//being written to (limit == capacity)
			final int maxFindBufferIter = 3;
			int findBufferIter = 0;
			for (; findBufferIter < maxFindBufferIter
					&& buffer.capacity() - 1 - currentWritePosition.bufferOffset() < eventSize; ++findBufferIter) {
				if (isDebugEnabled) {
					log.debug("skipping buffer " + currentWritePosition.bufferIndex() + ": " + buffer
						+ ": insufficient capacity " + (buffer.capacity() - currentWritePosition.bufferOffset())
						+ " < " + eventSize);
				}
				final long newWritePos = bufferPositionParser.incrementIndex(currentWritePosition.getPosition(),
						buffers);

				// ensureFreeSpace will call moveHead, which also resets limit to capacity
				ensureFreeSpace(currentWritePosition.getPosition(), newWritePos, isDebugEnabled);
				moveCurrentWritePosition(newWritePos);

				buffer = buffers[currentWritePosition.bufferIndex()];
			}

			if (maxFindBufferIter == findBufferIter) {
				throw new PtubesRunTimeException("insufficient buffer capacity for event of size:" + eventSize);
			}

			// passing true for noLimit, because we just need to make sure we don't go over capacity,
			// limit will be reset in the next call
			final long stopIndex = bufferPositionParser.incrementOffset(currentWritePosition.getPosition(),
				eventSize, buffers, true); //no limit true - see DDSDBUS-1515
			ensureFreeSpace(currentWritePosition.getPosition(), stopIndex, isDebugEnabled);
			buffer.position(currentWritePosition.bufferOffset());
		} finally {
			releaseWriteLock();
		}
	}

	@Override
	public void rollbackEvents() {
		acquireWriteLock();

		try {
			if (isClosed()) {
				LOG.warn("attempt to rollbackEvents for a closed buffer");
				return;
			}
			// do nothing
			// tail should be pointing to eventWindowStartBinlogInfo
			// reset window local state
			resetWindowState();
			rollbackCurrentWritePosition();
		} finally {
			releaseWriteLock();
		}
	}

	/**
	 * Reset currentWritePosition to tail; used for rolling back events written to the buffer. We also have to reset the
	 * buffer limits of buffers between tail and currentWritePosition. We have to watch out for the following special
	 * cases (1) [ CWP T ] - we have to reset all limits since it is guaranteed to be [ CWP H T ] unless other invariants
	 * are broken (2) [ T CWP H ] or [ T CWP ] [H] we should not reset any limits (3) [ T ][ CWP H ] - we should be
	 * careful not to reset the CWP ByteBuffer's limit.
	 **/
	private void rollbackCurrentWritePosition() {
		final int tailIdx = tail.bufferIndex();
		final int writePosIdx = currentWritePosition.bufferIndex();

		for (int i = 0; i < buffers.length; ++i) {
			final int realBufIdx = (tailIdx + i) % buffers.length;
			if (realBufIdx == tailIdx) {
				if (realBufIdx == writePosIdx && tail.bufferOffset() <= currentWritePosition.bufferOffset()) {
					//same ByteBuffer [ T CWP ] - no need to reset limit
					break;
				}
			}

			if (realBufIdx == writePosIdx && realBufIdx != tailIdx) {
				//we've reached the currentWritePosition; stop unless it is the case [ CWP T ]
				break;
			}

			buffers[realBufIdx].limit(buffers[realBufIdx].capacity());
		}

		currentWritePosition.setPosition(tail.getPosition());
		assert assertBuffersLimits();
	}

	public void endEvents(BinlogInfo binlogInfo) {
		if (eventState != EventBufferConstants.WindowState.EVENTS_ADDED) {
			//We set eventStartIndex here because currentWritePosition is not finalized before the call to prepareForAppend
			eventStartIndex.copy(currentWritePosition);
		}

		if (EventBufferConstants.WindowState.ENDED == eventState) {
			if (log.isDebugEnabled()) {
				log.debug("Skipping event window as Window is already in ended state" + binlogInfo);
			}
		}

		acquireWriteLock();

		try {
			if (isClosed()) {
				throw new PtubesRunTimeException("refusing to endEvents, because the buffer is closed");
			}

			if (EventBufferConstants.WindowState.STARTED == eventState && binlogInfo.equals(lastWrittenBinlogInfo)) {
				//nothing to finish
				if (log.isDebugEnabled()) {
					log.debug("Skipping event window that did not move forward:" + binlogInfo);
				}
				eventState = EventBufferConstants.WindowState.ENDED;

				return;
			}

			eventStartIndex.sanitize();
			InternalEventIterator eventIterator = acquireInternalIterator(eventStartIndex.getPosition(),
					currentWritePosition.getPosition(), "endEventsIterator");

			try {
				long firstEventPosition = eventIterator.getCurrentPosition();
				binlogInfoIndex.onTxn(binlogInfo, firstEventPosition);
				if (log.isDebugEnabled()) {
					log.debug("acquired iterator");
				}

				timestampOfFirstEvent = binlogInfo.getTs();
				timestampOfLatestDataEvent = binlogInfo.getTs();
			} finally {
				releaseIterator(eventIterator);
			}

			if (EventBufferConstants.QueuePolicy.OVERWRITE_ON_WRITE == memConfig.getQueuePolicy()) {
				try {
					binlogInfoIndex.assertLastWrittenPos(eventStartIndex);
				} catch (RuntimeException re) {
					log.error("Got runtime Exception: ", re);
					log.error("Event Buffer is: " + toString());
					log.error("BinlogInfo Index is: ");
					binlogInfoIndex.printVerboseString();
					throw re;
				}
			}

			eventState = EventBufferConstants.WindowState.ENDED;
			lastWrittenBinlogInfo = binlogInfo;
			long oldTail = tail.getPosition();

			assert currentWritePosition.bufferGenId() - head.bufferGenId() <= 1 : toString();

			tail.copy(currentWritePosition);
			if (head.getPosition() < 0) {
				head.setPosition(0);
			}

			if (EventBufferConstants.QueuePolicy.OVERWRITE_ON_WRITE == memConfig.getQueuePolicy()) {
				try {
					bufferPositionParser.assertSpan(head.getPosition(), tail.getPosition(), log.isDebugEnabled());
					binlogInfoIndex.assertHeadPosition(head.getRealPosition());
				} catch (RuntimeException re) {
					log.error("Got runtime Exception: ", re);
					log.info("Old Tail was: " + bufferPositionParser.toString(oldTail, buffers) + ", New Tail is: "
							+ tail.toString());
					log.error("Event Buffer is: " + toString());
					throw re;
				}
			}

			if (log.isDebugEnabled()) {
				log.debug("PtubesEventBuffer: head = " + head.toString() + " tail = " + tail.toString() + "isEmpty = "
						+ empty());
			}

			
			isEmpty = false;

			updateFirstEventMetadata();
			notEmpty.signalAll();

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			releaseWriteLock();
		}
	}

	@Override
	public boolean empty() {
		return (isEmpty);
	}

	protected void releaseWriteLock() {
		queueLock.unlock();
	}

	private void acquireWriteRangeLock(long startOffset, long endOffset) throws InterruptedException, TimeoutException {
		rwLockProvider.acquireWriterLock(startOffset, endOffset, bufferPositionParser);
	}

	private void releaseWriteRangeLock() {
		rwLockProvider.releaseWriterLock(bufferPositionParser);
	}

	protected void acquireWriteLock() {
		queueLock.lock();
	}

	public int getReadStatus() {
		return readLocked.get();
	}

	/**
	 * Makes sure that we have enough space at the destination write buffer. Depending on the queuing policy, we can
	 * either wait for space to free up or will overwrite it.
	 *
	 * @param writeStartPos
	 * 		the gen-id starting write position
	 * @param writeEndPos
	 * 		the gen-id ending write position (after the last byte written)
	 * @param logDebugEnabled
	 * 		a flag if debug logging messages are enabled
	 * @return true if the wait for free space was interrupted prematurely
	 */
	private boolean ensureFreeSpace(long writeStartPos, long writeEndPos, boolean logDebugEnabled) {
		// Normalize the write end position to make sure that if we have to move
		// the head, it points to real data.  This deals with two code-path
		// variants of the same basic bug.  Specifically, if the head points at the
		// current bytebuffer's limit (i.e., at invalid data at the end of it), and
		// the proposed end position is exactly equal to that, BufferPositionParser's
		// incrementOffset() (a.k.a. sanitize()) advances the position to the start
		// of the next buffer.  Since this is beyond (or "overruns") the head, the
		// subsequent call to setNextFreePos(), which calls moveCurrentWritePosition(),
		// blows up.  By normalizing the end position, we effectively block until the
		// head can advance to the same position (or beyond), i.e., until all iterators
		// have caught up, which allows setNextFreePos()/moveCurrentWritePosition() to
		// succeed.  See DDSDBUS-1816 for even more details.
		final BufferPosition normalizedWriteEndPos = new BufferPosition(writeEndPos, bufferPositionParser, buffers);
		normalizedWriteEndPos.skipOverFreeSpace();

		boolean interrupted = false;

		if (!empty()) {
			if (EventBufferConstants.QueuePolicy.BLOCK_ON_WRITE == memConfig.getQueuePolicy()) {
				interrupted = waitForReadEventsFreeSpace(logDebugEnabled, writeStartPos,
						normalizedWriteEndPos.getPosition());
			} else {
				freeUpSpaceForReadEvents(logDebugEnabled, writeStartPos, normalizedWriteEndPos.getPosition());
			}
		}
		if (logDebugEnabled) {
			log.debug("ensureFreeSpace: writeStart:" + bufferPositionParser.toString(writeStartPos, buffers)
					+ "; writeEnd:" + bufferPositionParser.toString(writeEndPos, buffers) + "; normalizedWriteEnd:"
					+ normalizedWriteEndPos + "; head:" + head + "; tail:" + tail + "; interrupted:" + interrupted);
		}
		assert interrupted || !overwritesHead(writeStartPos, normalizedWriteEndPos.getPosition());

		return interrupted;
	}

	/**
	 * Waits for space to free up in the buffer to be used by readEvents. The caller should specify the buffer range it
	 * wants to write to. The beginning and end of the range are genid-based buffer offsets.
	 *
	 * @param logDebugEnabled
	 * 		if we should log debug messages
	 * @param writeStartPosition
	 * 		the beginning of the desired write range
	 * @param writeEndPosition
	 * 		the end (after last byte) of the desired write range
	 * @return true if the wait succeeded; false if the wait was interrupted
	 */
	private boolean waitForReadEventsFreeSpace(boolean logDebugEnabled, long writeStartPosition,
			long writeEndPosition) {
		assert queueLock.isHeldByCurrentThread();

		boolean interrupted = false;
		// If we detect that we are overwriting head, wait till we have space available
		while (!interrupted && overwritesHead(writeStartPosition, writeEndPosition)) {
			log.info("Waiting for more space to be available. WriteStart: " + bufferPositionParser
					.toString(writeStartPosition, buffers) + " to " + bufferPositionParser
					.toString(writeEndPosition, buffers) + " head = " + head);

			try {
				notFull.await();
			} catch (InterruptedException ie) {
				log.warn("readEvents interrupted", ie);
				interrupted = true;
			}

			if (logDebugEnabled) {
				log.debug("Coming out of wait for more space. WriteStart: " + bufferPositionParser
						.toString(writeStartPosition, buffers) + " to " + bufferPositionParser
						.toString(writeEndPosition, buffers) + " head = " + head);
			}
			if (isClosed()) {
				throw new PtubesRunTimeException("Coming out of wait for more space, but buffer has been closed");
			}

		}
		return interrupted;
	}

	/**
	 * Checks if a given range of gen-id positions contains the head. The range is defined by [writeStartPosition,
	 * writeEndPosition). This check is performed to ensure that a write will not overwrite event data.
	 *
	 * @return true iff the given range contains the head
	 */
	private boolean overwritesHead(long writeStartPosition, long writeEndPosition) {
		return empty() ? false : Range.containsReaderPosition(writeStartPosition, writeEndPosition, head.getPosition(),
				bufferPositionParser);
	}

	/**
	 * Frees up in the buffer to be used by readEvents. This should be used only with OVERWRITE_ON_WRITE
	 * The caller should specify the buffer range it wants to write to. The beginning and end of the
	 * range are genid-based buffer offsets.
	 *
	 * Side effects: (1) the buffer head will be moved. (2) {@link #timestampOfFirstEvent} is changed
	 *
	 * @param logDebugEnabled
	 * 		if we should log debug messages
	 * @param writeStartPosition
	 * 		the beginning of the desired write range
	 * @param writeEndPosition
	 * 		the end (after last byte) of the desired write range
	 */
	private void freeUpSpaceForReadEvents(boolean logDebugEnabled, long writeStartPosition, long writeEndPosition) {
		if (logDebugEnabled) {
			log.debug("freeUpSpaceForReadEvents: start:" + bufferPositionParser.toString(writeStartPosition, buffers)
					+ "; end:" + bufferPositionParser.toString(writeEndPosition, buffers) + "; head:" + head);
		}
		if (overwritesHead(writeStartPosition, writeEndPosition)) {
			if (logDebugEnabled) {
				log.debug("free space from " + bufferPositionParser.toString(writeStartPosition, buffers) + " to "
						+ bufferPositionParser.toString(writeEndPosition, buffers) + " head = " + head);
			}

			long proposedHead = binlogInfoIndex.getLargerOffset(writeEndPosition);
			if (proposedHead < 0) {
				// Unless there is a bug in binlogInfo index code, the reason to get here is if
				// the transaction is too big to fit in one buffer.
				String error = "track(BinlogInfoIndex.head): failed to get larger window offset:" + "nextFreePosition="
						+ bufferPositionParser.toString(writeEndPosition, buffers) + " ;Head=" + head + "; Tail="
						+ tail + " ;CurrentWritePosition=" + currentWritePosition + " ;MinBinlogInfo="
						+ getMinBinlogInfo();
				log.error(error);
				binlogInfoIndex.printVerboseString();

				throw new PtubesRunTimeException(error);
			}

			//we need to fetch the binlogInfo for the new head to pass to BinlogInfoIndex.moveHead()
			
			BinlogInfo newBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
			long newTs = -1;
			if (proposedHead < tail.getPosition()) {
				PtubesEvent e = eventAtPosition(proposedHead);
				newBinlogInfo = e.getBinlogInfo();
				newTs = e.getTimestampInNS();
			}

			moveHead(proposedHead, newBinlogInfo, newTs, logDebugEnabled);
		}
	}

	private void adjustByteBufferLimit(long oldHeadPos) {
		final int newHeadIdx = head.bufferIndex();
		final int newHeadOfs = head.bufferOffset();
		final long newHeadGenid = head.bufferGenId();
		final int oldHeadIdx = bufferPositionParser.bufferIndex(oldHeadPos);
		final int oldHeadOfs = bufferPositionParser.bufferOffset(oldHeadPos);
		final long oldHeadGenid = bufferPositionParser.bufferGenId(oldHeadPos);

		assert oldHeadPos <= head.getPosition() : "oldHeaPos:" + oldHeadPos + " " + toString();
		assert newHeadGenid - oldHeadGenid <= 1 : "oldHeaPos:" + oldHeadPos + " " + toString();

		final boolean resetLimit = (newHeadIdx != oldHeadIdx) || /* head moves to a different ByteBuffer */
				(newHeadOfs < oldHeadOfs); /* wrap around in the same ByteBuffer */

		if (resetLimit) {
			if (buffers.length == 1) {
				//a special case of a wrap-around in a single buffer
				buffers[0].limit(buffers[0].capacity());
			} else {
				final int bufferNumDiff = (oldHeadGenid < newHeadGenid) ?
						buffers.length + newHeadIdx - oldHeadIdx : newHeadIdx - oldHeadIdx;
				assert 0 <= bufferNumDiff && bufferNumDiff <= buffers.length :
						"oldHeadPos:" + oldHeadPos + " " + toString();

				for (int i = 0; i < bufferNumDiff; ++i) {
					final int bufIdx = (oldHeadIdx + i) % buffers.length;
					buffers[bufIdx].limit(buffers[bufIdx].capacity());
				}
			}
			assert assertBuffersLimits();
		}
	}

	/**
	 * Creates an event at given gen-id position in the buffer. The position must be a valid position.
	 *
	 * @param pos
	 * 		the desired position
	 * @return the event object
	 */
	private PtubesEvent eventAtPosition(long pos) {
		final int proposedHeadIdx = bufferPositionParser.bufferIndex(pos);
		final int proposedHeadOfs = bufferPositionParser.bufferOffset(pos);
		PtubesEvent e = EventFactory.createReadOnlyEventFromBuffer(buffers[proposedHeadIdx], proposedHeadOfs);
		assert e.isValid();
		return e;
	}

	/**
	 * Moves the head of the buffer after an erase of events. Caller must hold {@link #queueLock}
	 *
	 * @param proposedHead
	 * 		the new head gen-id position
	 * @param binlogInfo
	 * 		the new head binlogInfo (may be -1)
	 * @param newHeadTsNs
	 * 		the new head timestamp (may be -1)
	 * @param logDebugEnabled
	 * 		if debuf logging is neabled
	 */
	protected void moveHead(long proposedHead, BinlogInfo binlogInfo, long newHeadTsNs, boolean logDebugEnabled) {
		assert queueLock.isHeldByCurrentThread();

		final long oldHeadPos = head.getPosition();
		if (logDebugEnabled) {
			log.debug(
				"about to move head to " + bufferPositionParser.toString(proposedHead, buffers) + "; binlogInfo="
					+ binlogInfo + "; oldhead=" + head);
		}

		try {
			acquireWriteRangeLock(oldHeadPos, proposedHead);
		} catch (InterruptedException e) {
			throw new PtubesRunTimeException(e);
		} catch (TimeoutException e) {
			throw new PtubesRunTimeException(e);
		}

		try {
			if (proposedHead > tail.getPosition()) {
				throw new PtubesRunTimeException(
					"moveHead assert failure: newHead > tail: newHead:" + proposedHead + " " + toString());
			}
			if (tail.bufferGenId() - bufferPositionParser.bufferGenId(proposedHead) > 1) {
				throw new PtubesRunTimeException(
					"moveHead assert failure: gen mismatch: newHead:" + proposedHead + " " + toString());
			}

			this.setPrevBinlogInfo(getMinBinlogInfo());
			head.setPosition(proposedHead);
			if (head.equals(tail)) {
				isEmpty = true;
				binlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
			}

			if (null != binlogInfoIndex) {
				binlogInfoIndex.moveHead(head.getPosition(), binlogInfo);
			}

			updateFirstEventMetadata();

			//next we make sure we preserve the ByteBuffer limit() invariant -- see the comment
			//to buffers
			adjustByteBufferLimit(oldHeadPos);

			if (logDebugEnabled) {
				log.debug("moved head to " + head.toString() + "; binlogInfo=" + binlogInfo);
			}
			notFull.signalAll();
		} finally {
			releaseWriteRangeLock();
		}
	}

	// We could probably optimize this to update minBinlogInfo and timetampOfFirstEvent
	// only if they are not set OR if we are moving head.
	private void updateFirstEventMetadata() {
		if (!queueLock.isHeldByCurrentThread()) {
			throw new RuntimeException("Queue lock not held when updating minBinlogInfo");
		}
		boolean found = false;
		BaseEventIterator it = null;
		try {
			it = this.acquireLockFreeInternalIterator(head.getPosition(), tail.getPosition(),
					"updateFirstEventMetadata");
			while (it.hasNext()) {
				PtubesEvent e = it.next();
				minBinlogInfo = e.getBinlogInfo();
				timestampOfFirstEvent = e.getTimestampInNS() / 1000000;
				found = true;
				break;
			}
			if (!found) {
				minBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
				timestampOfFirstEvent = 0;
			}
		} finally {
			if (null != it) {
				it.close();
			}
		}
	}

	/**
	 * Moves currentWritePosition
	 *
	 * @param newWritePos
	 * 		new gen-id position value for currentWritePosition
	 */
	protected void moveCurrentWritePosition(long newWritePos) {
		//no write position regressions
		if (currentWritePosition.getPosition() >= newWritePos) {
			throw new PtubesRunTimeException(
					"moveCurrentWritePosition: assert regression: " + " currentWritePosition:" + currentWritePosition
							+ "; newWritePos:" + bufferPositionParser.toString(newWritePos, buffers));
		}
		//make sure head is not overwritten
		if (overwritesHead(currentWritePosition.getPosition(), newWritePos)) {
			throw new PtubesRunTimeException("moveCurrentWritePosition: overwritesHead assert:" + this);
		}

		final int curWriteIdx = currentWritePosition.bufferIndex();
		final int curWriteOfs = currentWritePosition.bufferOffset();
		final long curWriteGenid = currentWritePosition.bufferGenId();
		final int newWriteIdx = bufferPositionParser.bufferIndex(newWritePos);
		final int newWriteOfs = bufferPositionParser.bufferOffset(newWritePos);
		final long newWriteGenid = bufferPositionParser.bufferGenId(newWritePos);

		//don't skip ByteBuffers
		if (!(newWriteIdx == curWriteIdx || ((curWriteIdx + 1) % buffers.length) == newWriteIdx)) {
			throw new PtubesRunTimeException(
					"buffer skip: currentWritePosition:" + currentWritePosition + "; newWritePos:"
							+ bufferPositionParser.toString(newWritePos, buffers));
		}
		//don't skip generations
		if (newWriteGenid - curWriteGenid > 1) {
			throw new PtubesRunTimeException(
					"generation skip: currentWritePosition:" + currentWritePosition + "; newWritePos:"
							+ bufferPositionParser.toString(newWritePos, buffers) + "; this=" + this);
		}

		if ((newWriteGenid - head.bufferGenId()) > 1) {
			throw new PtubesRunTimeException(
					"new write position too far ahead: " + bufferPositionParser.toString(newWritePos, buffers)
							+ "; this=" + this);
		}

		// move to a new ByteBuffer or wrap around in current?
		final boolean resetLimit = newWriteIdx != curWriteIdx || newWriteOfs < curWriteOfs;
		if (resetLimit) {
			buffers[curWriteIdx].limit(curWriteOfs);
		}

		currentWritePosition.setPosition(newWritePos);

		assert assertBuffersLimits();
	}

	/** Asserts the ByteBuffers limit() invariant. {@see #buffers} */
	boolean assertBuffersLimits() {
		boolean success = tail.getPosition() <= currentWritePosition.getPosition();

		if (!success) {
			log.error("tail:" + tail + "> currentWritePosition:" + currentWritePosition);
			return false;
		}

		final int headIdx = head.bufferIndex();
		final int writeIdx = currentWritePosition.bufferIndex();

		// Buffers are split into zones depending on their relative position to the head and
		// currentWritePosition
		// head Zone1 currentWritePosition Zone2
		// Buffers in Zone2 are not full and should have their limit() == capacity()
		int zone = head.getPosition() == currentWritePosition.getPosition() ? 2 : 1;
		for (int i = 0; i < buffers.length; ++i) {
			final int bufIdx = (headIdx + i) % buffers.length;
			if (1 == zone && bufIdx == writeIdx) {
				//should we move to Zone 2?
				//just make sure that if the H and CWP are in the same buffer, H is before CWP
				if (bufIdx != headIdx || head.bufferOffset() < currentWritePosition.bufferOffset()) {
					zone = 2;
				}
			}

			if (2 == zone) { // isEmpty zone, not readable
				if (buffers[bufIdx].limit() != buffers[bufIdx].capacity()
						// in isEmpty zone limit should be equal to capacity
						&& head.bufferOffset() != buffers[bufIdx]
						.limit() // unless head is at the limit (at the end but wasn't sanitized yet)
						) {
					success = false;
					log.error(
							"assertBuffersLimits failure: buf[" + bufIdx + "]=" + buffers[bufIdx] + "; head:" + head
									+ "; currentWritePosition:" + currentWritePosition + "; tail:" + tail);
				}
			}
		}

		return success;
	}

	@Override
	public String toString() {
		return "PtubesEventBuffer [" + ",rwLockProvider=" + rwLockProvider + ", readLocked=" + readLocked
				+ ",binlogInfoIndex=" + binlogInfoIndex + ", buffers=" + Arrays.toString(buffers) + ",memConfig="
				+ memConfig + "," + bufPositionInfo() + ",allocatedSize=" + allocatedSize + ", internalListeners="
				+ internalListeners + ",mmapSessionDirectory=" + mmapSessionDirectory + ",busyIteratorPool.size="
				+ busyIteratorPool.size() + ", eventState=" + eventState + ",eventStartIndex=" + eventStartIndex
				+ ",lastWrittenBinlogInfo=" + lastWrittenBinlogInfo
				+ ", prevBinlogInfo=" + prevBinlogInfo + ",bufferPositionParser=" + bufferPositionParser + "]";
	}

	public String bufPositionInfo() {
		return "head=" + head + ",tail=" + tail + ",isEmpty=" + isEmpty + ",currentWritePosition="
				+ currentWritePosition;
	}

	/**
	 * Returns the amount of free space left in the event buffer. No guarantees of atomicity.
	 */
	public long getBufferFreeSpace() {
		long remaining = remaining();
		return remaining;
	}

	/**
	 * Returns the amount of space left in the buffer that can be safely read from a channel. No guarantees of atomicity.
	 */
	public int getBufferFreeReadSpace() {
		// While in readEvents, _readBuffer could be in inconsistent state

		assert (eventState != EventBufferConstants.WindowState.IN_READ);

		long remaining = remaining();
		return (int) Math.min(remaining, getMaxReadBufferCapacity());
	}

	public int getMaxReadBufferCapacity() {
		return memConfig.getMaxIndividualBufferSizeInByte();
	}

	private long remaining() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Remaining query : head = " + head.toString() + " tail =" + tail.toString());
		}
		if (empty()) {
			long space = 0;
			for (ByteBuffer buf : buffers) {
				space += buf.capacity();
			}
			return space;
		}

		if (head.getRealPosition() < tail.getRealPosition()) {
			long space = 0;
			for (int i = 0; i < head.bufferIndex(); ++i) {
				space += buffers[i].capacity();
			}
			space += head.bufferOffset();
			space += buffers[tail.bufferIndex()].capacity() - tail.bufferOffset();
			for (int i = tail.bufferIndex() + 1; i < buffers.length; ++i) {
				space += buffers[i].capacity();
			}
			return space;
		}

		if (head.getRealPosition() > tail.getRealPosition()) {
			if (head.bufferIndex() == tail.bufferIndex()) {
				return (head.getRealPosition() - tail.getRealPosition());
			} else {
				long space = buffers[tail.bufferIndex()].capacity() - tail.bufferOffset();
				space += head.bufferOffset();

				for (int i = tail.bufferIndex() + 1; i < head.bufferIndex(); ++i) {
					space += buffers[i].capacity();
				}
				return space;
			}

		}

		return 0;
	}

	
	//debugging and it has overhead associated with it
	protected void trackIterator(BaseEventIterator eventIterator) {
		synchronized (busyIteratorPool) {
			busyIteratorPool.add(new WeakReference<BaseEventIterator>(eventIterator));
		}
	}

	protected void untrackIterator(BaseEventIterator eventIterator) {
		synchronized (busyIteratorPool) {
			Iterator<WeakReference<BaseEventIterator>> refIter = busyIteratorPool.iterator();
			//both remove specified iterator and clean up GC'ed references
			while (refIter.hasNext()) {
				WeakReference<BaseEventIterator> curRef = refIter.next();
				BaseEventIterator iter = curRef.get();
				if (null == iter) {
					refIter.remove();
				} else if (iter.equals(eventIterator)) {
					refIter.remove();
				}
			}
		}
	}

	/**
	 * Creates a long-lived iterator over events. It has the ability to wait if there are no immediately-available events.
	 * It is responsibility of the caller to free the iterator using {@link #releaseIterator(BaseEventIterator)}.
	 */
	public PtubesEventIterator acquireIterator(long headPoint, long tailPoint, String iteratorName) {
		acquireWriteLock();
		try {
			PtubesEventIterator eventIterator = new PtubesEventIterator(headPoint, tailPoint,
					iteratorName);
			return eventIterator;
		} finally {
			releaseWriteLock();
		}
	}

	/**
	 * Acquires an iterator over a fixed range of events. This iterator cannot block waiting for more events. It is
	 * responsibility of the caller to free the iterator using {@link #releaseIterator(BaseEventIterator)}.
	 */
	public InternalEventIterator acquireInternalIterator(long head, long tail, String iteratorName) {
		InternalEventIterator eventIterator = new InternalEventIterator(head, tail, iteratorName);
		return eventIterator;
	}

	/**
	 * Acquires an iterator over a fixed range of events with no range-locking (it is responsibility of the caller to
	 * ensure this is safe, e.g., by holding another range-lock over the desired iterator range). This iterator cannot
	 * block waiting for more events. It is responsibility of the caller to free the iterator using
	 * {@link #releaseIterator(BaseEventIterator)}.
	 */
	protected BaseEventIterator acquireLockFreeInternalIterator(long head, long tail, String iteratorName) {
		BaseEventIterator eventIterator = new BaseEventIterator(head, tail, iteratorName);
		return eventIterator;
	}

	/**
	 * Creates a "short-lived" iterator. The set of events that it is going to iterate over is pre-determined at the time
	 * of the iterator creation. Subsequent additions of events to the buffer will not be visible.
	 *
	 * <b>Important: any resources associated with the iterator will be released once it goes over all events, i.e., when
	 * {@link Iterator#hasNext()} returns false.</b>
	 *
	 * The iterator is meant to be used mostly in for-each loops.
	 */
	@Override
	public Iterator<EventInternalWritable> iterator() {
		acquireWriteLock();
		try {
			ManagedEventIterator eventIterator = new ManagedEventIterator(head.getPosition(), tail.getPosition());
			return eventIterator;
		} finally {
			releaseWriteLock();
		}
	}

	public void releaseIterator(BaseEventIterator e) {
		e.close();
	}

	public void addInternalListener(InternalEventsListener listener) {
		if (!internalListeners.contains(listener)) {
			internalListeners.add(listener);
		}
	}

	public boolean removeInternalListener(InternalEventsListener listener) {
		return internalListeners.remove(listener);
	}

	/**
	 * package private to allow helper classes to inspect internal details
	 */
	long getHead() {
		return head.getPosition();
	}

	/**
	 * package private to allow helper classes to inspect internal details
	 */
	long getTail() {
		return tail.getPosition();
	}

	/**
	 * package private to allow helper classes to inspect internal details
	 */
	ByteBuffer[] getBuffer() {
		return buffers;
	}

	/**
	 * package private to allow helper classes to inspect internal details
	 */
	BinlogInfoIndex getBinlogInfoIndex() {
		return binlogInfoIndex;
	}

	/**
	 * @return the bufferPositionParser for this event buffer
	 */
	public BufferPositionParser getBufferPositionParser() {
		return bufferPositionParser;
	}

	/**
	 * package private to allow helper classes to set the head of the buffer internally updates index state as well.
	 *
	 * <p>
	 * <b>NOTE: This modifies the event buffer state directly. Use outside unit tests is extremely discouraged</b>
	 */
	void setHead(long offset) {
		head.setPosition(offset);
		binlogInfoIndex.moveHead(offset);
	}

	/**
	 * package private to allow helper classes to set the tail of the buffer this does not update the binlogInfoIndex
	 *
	 * <p>
	 * <b>NOTE: This modifies the event buffer state directly. Use outside unit tests is extremely discouraged</b>
	 */
	void setTail(long offset) {
		tail.setPosition(offset);
		currentWritePosition.setPosition(offset);
	}

	/**
	 * deletes at least the first window in the buffer Useful if you want to align the head of the buffer past an eop
	 * marker
	 *
	 * @return -1 if we couldn't find the next window
	 */
	long deleteFirstWindow() {
		long proposedHead = binlogInfoIndex.getLargerOffset(head.getPosition());
		if (proposedHead > 0) {
			head.setPosition(proposedHead);
			binlogInfoIndex.moveHead(proposedHead);
		}
		return proposedHead;
	}

	public long getAllocatedSize() {
		return allocatedSize;
	}

	@Override
	public BinlogInfo getLastWrittenBinlogInfo() {
		return lastWrittenBinlogInfo;
	}

	@Override
	public void setStartBinlogInfo(BinlogInfo binlogInfo) {
		setPrevBinlogInfo(binlogInfo);
	}

	/** Use only for testing!! sets isEmpty state of buffer */
	void setEmpty(boolean val) {
		isEmpty = val;
	}

	public long getTimestampOfLatestDataEvent() {
		return timestampOfLatestDataEvent;
	}

	/**
	 * perform various closing duties
	 *
	 * @param persistBuffer
	 * 		- if true save metainfo, if false delete sessionId dir and metainfo file
	 */
	public void closeBuffer(boolean persistBuffer) {

		acquireWriteLock();
		try {

			// after this try-catch no new modifications of the buffer should start
			// but we need to check for some modifications in progress
			if (eventState != EventBufferConstants.WindowState.ENDED
					&& eventState != EventBufferConstants.WindowState.INIT) { // ongoing append
				rollbackEvents();
			}

			setClosed(); // done under writelock

		} catch (PtubesException e) {
			log.warn("for buffer " + toString(), e);
			return; // buffer already closed
		} finally {
			releaseWriteLock();
		}

		// some listeners are appenders to a file
		if (internalListeners != null) {
			for (InternalEventsListener l : internalListeners) {
				try {
					l.close();
				} catch (IOException ioe) {
					log.warn("Couldn't close channel/file for listener=" + l, ioe);
				} catch (RuntimeException re) {
					log.warn("Couldn't close channel/file for listener=" + l, re);
				}
			}
		}

		if (persistBuffer) {
			try {
				saveBufferMetaInfo(false);
			} catch (IOException e) {
				log.error("error saving meta info for buffer for partition: " + readerTaskName + ": " + e
						.getMessage(), e);
			} catch (RuntimeException e) {
				log.error("error saving meta info for buffer for partition: " + readerTaskName + ": " + e
						.getMessage(), e);
			}
		} else {
			// remove the content of mmap directory and the directory itself
			cleanUpPersistedBuffers();
		}
	}

	private void flushMMappedBuffers() {
		log.info("flushing buffers to disk for partition: " + readerTaskName + "; allocation_policy=" + memConfig
				.getAllocationPolicy());
		if (memConfig.getAllocationPolicy() == EventBufferConstants.AllocationPolicy.MMAPPED_MEMORY) {
			for (ByteBuffer buf : buffers) {
				if (buf instanceof MappedByteBuffer) {
					((MappedByteBuffer) buf).force();
				}
			}

			binlogInfoIndex.flushMMappedBuffers();
			log.info("done flushing buffers to disk for partition: " + readerTaskName);
		}
	}

	public void cleanUpPersistedBuffers() {
		cleanUpPersistedBuffers(mmapSessionDirectory, mmapDirectory);
	}

	public void cleanUpPersistedBuffers(File sessionDir, File mmapDir) {
		if (memConfig.getAllocationPolicy() != EventBufferConstants.AllocationPolicy.MMAPPED_MEMORY) {
			log.info("Not cleaning up buffer mmap directory because allocation policy is " + memConfig
					.getAllocationPolicy() + "; bufferPersistenceEnabled:" + memConfig.isRestoreMMappedBuffers());
			return;
		}

		// remove all the content of the session id for this buffer
		log.warn("Removing mmap directory for buffer(" + readerTaskName + "): " + sessionDir);
		if (!sessionDir.exists() || !sessionDir.isDirectory()) {
			log.warn(
					"cannot cleanup _mmap=" + sessionDir + " directory because it doesn't exist or is not a directory");
			return;
		}

		try {
			FileUtils.cleanDirectory(sessionDir);
		} catch (IOException e) {
			log.error("failed to cleanup buffer session directory " + mmapSessionDirectory);
		}
		// delete the directory itself
		if (!sessionDir.delete()) {
			log.error("failed to delete buffer session directory " + mmapSessionDirectory);
		}

		File metaFile = new File(mmapDir, metaFileName());
		if (metaFile.exists()) {
			log.warn("Removing meta file " + metaFile);
			if (!metaFile.delete()) {
				log.error("failed to delete metafile " + metaFile);
			}
		}
	}

	/**
	 * save metaInfoFile about the internal buffers + binlogInfo index.
	 *
	 * @param infoOnly
	 * 		- if true, will create a meta file that will NOT be used when loading the buffers
	 * @throws IOException
	 */

	public void saveBufferMetaInfo(boolean infoOnly) throws IOException {

		acquireWriteLock(); // uses _queue lock, same lock used by readevents()
		try {
			// first make sure to flush all the data including binlogInfo index data
			flushMMappedBuffers();

			// save index metadata
			//  binlogInfoIndex file will be located in the session directory
			binlogInfoIndex.saveBufferMetaInfo();

			saveDataBufferMetaInfo(infoOnly);
		} finally {
			releaseWriteLock();
		}
	}

	private void saveDataBufferMetaInfo(boolean infoOnly) throws IOException {

		if (memConfig.getAllocationPolicy() != EventBufferConstants.AllocationPolicy.MMAPPED_MEMORY || !memConfig
				.isRestoreMMappedBuffers()) {
			log.info("Not saving state metaInfoFile, because allocation policy is " + memConfig.getAllocationPolicy()
					+ "; bufferPersistenceEnabled:" + memConfig.isRestoreMMappedBuffers());
			return;
		}

		String fileName = metaFileName() + (infoOnly ? MMAP_META_INFO_SUFFIX : "");
		PtubesEventBufferMetaInfo mi = new PtubesEventBufferMetaInfo(new File(mmapDirectory, fileName));
		log.info("about to save PtubesEventBuffer for PP " + readerTaskName + " state into " + mi.toString());

		// record session id - to figure out directory for the buffers
		mi.setSessionId(sessionId);

		// write buffers specific info - num of buffers, pos and limit of each one
		mi.setVal(PtubesEventBufferMetaInfo.NUM_BYTE_BUFFER, Integer.toString(buffers.length));
		StringBuilder bufferInfo = new StringBuilder("");
		for (ByteBuffer b : buffers) {
			PtubesEventBufferMetaInfo.BufferInfo bi = new PtubesEventBufferMetaInfo.BufferInfo(b.position(),
					b.limit(), b.capacity());
			bufferInfo.append(bi.toString());
			bufferInfo.append(" ");
		}
		mi.setVal(PtubesEventBufferMetaInfo.BYTE_BUFFER_INFO, bufferInfo.toString());

		String currentWritePosition = Long.toString(this.currentWritePosition.getPosition());
		mi.setVal(PtubesEventBufferMetaInfo.CURRENT_WRITE_POSITION, currentWritePosition);

		// _maxBufferSize
		mi.setVal(PtubesEventBufferMetaInfo.MAX_BUFFER_SIZE, Integer.toString(memConfig.getMaxIndexSizeInByte()));

		//NOTE. no need to save readBuffer and rwChannel

		String head = Long.toString(this.head.getPosition());
		mi.setVal(PtubesEventBufferMetaInfo.BUFFER_HEAD, head);

		String tail = Long.toString(this.tail.getPosition());
		mi.setVal(PtubesEventBufferMetaInfo.BUFFER_TAIL, tail);

		String empty = Boolean.toString(this.isEmpty);
		mi.setVal(PtubesEventBufferMetaInfo.BUFFER_EMPTY, empty);

		mi.setVal(PtubesEventBufferMetaInfo.ALLOCATED_SIZE, Long.toString(allocatedSize));

		mi.setVal(PtubesEventBufferMetaInfo.EVENT_START_INDEX, Long.toString(eventStartIndex.getPosition()));

		// lastWrittenBinlogInfo
		mi.setVal(PtubesEventBufferMetaInfo.LAST_WRITTEN_BINLOGINFO, lastWrittenBinlogInfo.toString());

		mi.setVal(PtubesEventBufferMetaInfo.SEEN_END_OF_PERIOD_BINLOGINFO, seenEndOfPeriodBinlogInfo.toString());
		// prevBinlogInfo
		mi.setVal(PtubesEventBufferMetaInfo.PREV_BINLOGINFO, prevBinlogInfo.toString());
		// timestampOfFirstEvent
		mi.setVal(PtubesEventBufferMetaInfo.TIMESTAMP_OF_FIRST_EVENT, Long.toString(timestampOfFirstEvent));
		// timestampOfLatestDataEvent
		mi.setVal(PtubesEventBufferMetaInfo.TIMESTAMP_OF_LATEST_DATA_EVENT,
				Long.toString(timestampOfLatestDataEvent));
		// eventState
		mi.setVal(PtubesEventBufferMetaInfo.EVENT_STATE, eventState.toString());

		mi.saveAndClose();

	}

	public void initBuffersWithMetaInfo(PtubesEventBufferMetaInfo mi)
			throws PtubesEventBufferMetaInfo.PtubesEventBufferMetaInfoException {

		if (mi.isValid()) {
			head.setPosition(mi.getLong(PtubesEventBufferMetaInfo.BUFFER_HEAD));
			tail.setPosition(mi.getLong(PtubesEventBufferMetaInfo.BUFFER_TAIL));

			currentWritePosition.setPosition(mi.getLong(PtubesEventBufferMetaInfo.CURRENT_WRITE_POSITION));
			isEmpty = mi.getBool(PtubesEventBufferMetaInfo.BUFFER_EMPTY);
			// eventStartIndex
			eventStartIndex.setPosition(mi.getLong(PtubesEventBufferMetaInfo.EVENT_START_INDEX));
			eventState = EventBufferConstants.WindowState.valueOf(mi.getVal(PtubesEventBufferMetaInfo.EVENT_STATE));

			lastWrittenBinlogInfo = BinlogInfoFactory.newBinlogInfoByStr(sourceType, mi.getVal(PtubesEventBufferMetaInfo.LAST_WRITTEN_BINLOGINFO));
			seenEndOfPeriodBinlogInfo = BinlogInfoFactory.newBinlogInfoByStr(sourceType,
				mi.getVal(PtubesEventBufferMetaInfo.SEEN_END_OF_PERIOD_BINLOGINFO));

			prevBinlogInfo = BinlogInfoFactory.newBinlogInfoByStr(sourceType, mi.getVal(PtubesEventBufferMetaInfo.PREV_BINLOGINFO));
			timestampOfFirstEvent = mi.getLong(PtubesEventBufferMetaInfo.TIMESTAMP_OF_FIRST_EVENT);
			timestampOfLatestDataEvent = mi.getLong(PtubesEventBufferMetaInfo.TIMESTAMP_OF_LATEST_DATA_EVENT);
		}
	}

	public BinlogInfo getSeenEndOfPeriodBinlogInfo() {
		return seenEndOfPeriodBinlogInfo;
	}

	public Logger getLog() {
		return log;
	}

	/**
	 * Dumps as a hex string the contents of a buffer around a position. Useful for debugging purposes.
	 *
	 * @param pos
	 * 		gen-id position in the event buffer
	 * @param length
	 * 		number of bytes to print
	 * @return the string; if it starts with "!", an error occurred
	 */
	public String hexdumpByteBufferContents(long pos, int length) {
		try {
			if (length < 0) {
				return "! invalid length: " + length;
			}

			final int bufIdx = bufferPositionParser.bufferIndex(pos);
			if (bufIdx >= buffers.length) {
				return "! invalid buffer position: " + pos;
			}

			final int bufOfs = bufferPositionParser.bufferOffset(pos);
			return BufferUtil.hexdumpByteBufferContents(buffers[bufIdx], bufOfs, length);
		} catch (RuntimeException e) {
			return "! unable to generate dump for position " + pos + ": " + e;
		}
	}
	public synchronized byte getEventSerializationVersion() {
		return eventSerializationVersion;
	}

}
