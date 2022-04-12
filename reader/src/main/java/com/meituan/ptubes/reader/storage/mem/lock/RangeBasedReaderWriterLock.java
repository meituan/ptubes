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
package com.meituan.ptubes.reader.storage.mem.lock;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import com.meituan.ptubes.reader.storage.mem.buffer.BufferPositionParser;


public class RangeBasedReaderWriterLock {
	public static final String MODULE = RangeBasedReaderWriterLock.class.getName();
	public static final Logger LOG = LoggerFactory.getLogger(MODULE);

	private static final long MAX_LOCK_WAIT_MS = 500;

	public class LockToken implements Comparable<LockToken> {
		protected Range id;
		private final String ownerName;
		private final long createTime;
		private long lastUpdateTime;

		protected LockToken(Range id, String ownerName) {
			this.id = id;
			this.ownerName = ownerName;
			createTime = System.currentTimeMillis();
			lastUpdateTime = createTime;
		}

		public Range getRange() {
			return id;
		}

		public String getOwnerName() {
			return ownerName;
		}

		public void setRangeStart(long newStart) {
			id.start = newStart;
			lastUpdateTime = System.currentTimeMillis();
		}

		@Override
		public String toString() {
			return "{ownerName:" + ownerName + ", range:" + id +
					", created:" + createTime + ", lastUpdated:" + lastUpdateTime +
					"}";
		}

		public String toString(BufferPositionParser parser) {
			return "{ownerName:" + ownerName + ", range:" + id.toString(parser) + "}";
		}

		@Override
		public int compareTo(LockToken o) {
			return id.compareTo(o.id);
		}
	}

	private final PriorityQueue<LockToken> readerRanges;
	private final ReentrantLock mutex;
	private final Condition writesPossible;
	private final Condition readsPossible;

	private Range writerRange;
	private boolean writerIn;
	private boolean writerWaiting;

	public RangeBasedReaderWriterLock() {
		readerRanges = new PriorityQueue<LockToken>(100);
		writerRange = new Range(-1, 0);
		writerIn = false;
		mutex = new ReentrantLock();
		writesPossible = mutex.newCondition();
		readsPossible = mutex.newCondition();

	}

	public LockToken acquireReaderLock(long startOffset, long endOffset, BufferPositionParser parser, String ownerName)
			throws InterruptedException, TimeoutException {
		boolean debug = LOG.isDebugEnabled();
		if (debug) {
			LOG.debug("Asked to acquire reader lock from " + parser.toString(startOffset) +
					" to " + parser.toString(endOffset) + " for " + ownerName);
		}

		Range readerRange = new Range(startOffset, endOffset);
		mutex.lock();
		try {
			boolean timeout = false;
			while (writerIn && writerRange.intersects(readerRange)) {
				if (debug) {
					LOG.debug("Waiting for reads to be possible since writer is In. Reader Range is :" + readerRange
							.toString(parser) + ". Writer Range is :" + writerRange.toString(parser));
				}
				if (timeout) {
					TimeoutException e = new TimeoutException();
					LOG.warn(
						"Acquire read lock timeout, timeout: {}",
						MAX_LOCK_WAIT_MS,
						e
					);
					throw e;
				}

				if (!readsPossible.await(MAX_LOCK_WAIT_MS, TimeUnit.MILLISECONDS)) {
					LOG.info(
						"Wait for read lock timeout, timeout: {}, Reader Range is: {}, Writer Range is: {}",
						MAX_LOCK_WAIT_MS,
						readerRange
							.toString(parser),
						writerRange.toString(parser)
					);
					timeout = true;
				}

				if (debug) {
					LOG.info("Waiting for reads to be possible: coming out of wait");
				}
			}
			LockToken returnVal = new LockToken(readerRange, ownerName);
			readerRanges.add(returnVal);
			if (debug) {
				LOG.debug("Returning with reader lock from " + parser.toString(startOffset) + " to " + parser
						.toString(endOffset));
			}

			return returnVal;
		} finally {
			mutex.unlock();
		}

	}

	public void shiftReaderLockStart(LockToken lockId, long newStartOffset, BufferPositionParser parser) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Being asked to shift reader lock start to " + parser.toString(newStartOffset) +
					" for " + lockId);
		}
		mutex.lock();
		try {
			boolean lockFound = readerRanges.remove(lockId);
			assert lockFound : "lock:" + lockId + "; this:" + toString();
			lockId.setRangeStart(newStartOffset);
			readerRanges.add(lockId);
			writesPossible.signal();
		} finally {
			mutex.unlock();
		}

	}

	public void releaseReaderLock(LockToken lockId) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Being asked to release reader lock " + lockId);
		}

		mutex.lock();
		try {
			boolean readerLockRemoved = readerRanges.remove(lockId);
			assert readerLockRemoved : "lock:" + lockId + "; this:" + toString();
			writesPossible.signal();
		} finally {
			mutex.unlock();
		}
	}

	public void acquireWriterLock(long start, long end, BufferPositionParser parser)
			throws InterruptedException, TimeoutException {
		long startOffset = parser.address(start);
		long endOffset = parser.address(end);

		boolean debug = LOG.isDebugEnabled();

		if (debug) {
			LOG.debug("Acquiring writer lock from " + parser.toString(start) + " to " + parser.toString(end));
		}

		mutex.lock();

		try {
			boolean timeout = false;
			while (!readerRanges.isEmpty() && Range.contains(startOffset, endOffset,
					parser.address(readerRanges.peek().id.start))) {
				if (debug) {
					LOG.debug("Entering wait because reader(s) exist: Writer Range: [" + parser.toString(start)
							+ "(Address:" + parser.toString(startOffset) + ")-" + parser.toString(end) + "(Address:"
							+ parser.toString(endOffset) +
							")]. Nearest Reader Range :" + readerRanges.peek().toString(parser));
				}
				if (timeout) {
					LOG.error("timed out waiting for a write lock for [" + parser.toString(start) +
							"," + parser.toString(end) + "); this: " + this);
					throw new TimeoutException();
				}

				for (LockToken token : readerRanges) {
					LOG.info(token.toString(parser));
				}
				writerWaiting = true;
				if (!writesPossible.await(MAX_LOCK_WAIT_MS, TimeUnit.MILLISECONDS)) {
					LOG.info(
						"Wait for write lock timeout, timeout: {}, range: {} to {}",
						MAX_LOCK_WAIT_MS,
						parser.toString(start),
						parser.toString(end)
					);
					timeout = true;
				}

				if (debug) {
					LOG.debug("Writer coming out of wait");
				}
			}
			writerWaiting = false;
			writerIn = true;
			writerRange.start = start;
			writerRange.end = end;
		} finally {
			mutex.unlock();
		}

	}

	public void releaseWriterLock(BufferPositionParser parser) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Releasing writer lock from " + parser.toString(writerRange.start) + " to " + parser
					.toString(writerRange.end));
		}

		mutex.lock();
		try {
			writerIn = false;
			readsPossible.signalAll();
		} finally {
			mutex.unlock();
		}
	}

	public String toString(BufferPositionParser parser, boolean doSort) {
		StringBuilder strBuilder = new StringBuilder();

		strBuilder.append("[writerIn:" + writerIn).append(",WriterWaiting:");
		strBuilder.append(writerWaiting).append(",WriterRange:").append(writerRange.toString(parser));
		strBuilder.append("\nReader Ranges:\n");

		if (!doSort) {
			Iterator<LockToken> it = readerRanges.iterator();
			while (it.hasNext()) {
				strBuilder.append(it.next().toString(parser)).append("\n");
			}
		} else {
			LockToken[] ranges = new LockToken[readerRanges.size()];
			readerRanges.toArray(ranges);
			Arrays.sort(ranges);
			for (int i = 0; i < ranges.length; i++) {
				strBuilder.append(ranges[i].toString(parser)).append("\n");
			}
		}
		return strBuilder.toString();
	}

	// package private getters for unit-tests
	PriorityQueue<LockToken> getReaderRanges() {
		return readerRanges;
	}

	public boolean isWriterIn() {
		return writerIn;
	}

	public boolean isWriterWaiting() {
		return writerWaiting;
	}

	public Range getWriterRange() {
		return writerRange;
	}

	public int getNumReaders() {
		return readerRanges.size();
	}

	@Override
	public String toString() {
		mutex.lock();
		try {
			return "{readerRanges:" + readerRanges + ", writerRange:" + writerRange +
					", writerIn:" + writerIn + ", writerWaiting:" + writerWaiting + "}";
		} finally {
			mutex.unlock();
		}
	}
}
