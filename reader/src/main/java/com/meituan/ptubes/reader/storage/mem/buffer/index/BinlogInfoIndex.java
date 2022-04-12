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
package com.meituan.ptubes.reader.storage.mem.buffer.index;

import com.google.common.base.Preconditions;
import com.meituan.ptubes.common.exception.OffsetNotFoundException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.constants.AssertLevel;
import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.mem.buffer.BinlogInfoFactory;
import com.meituan.ptubes.reader.storage.mem.buffer.PtubesEventBuffer;
import com.meituan.ptubes.reader.storage.mem.buffer.PtubesEventBufferMetaInfo;
import com.meituan.ptubes.reader.storage.mem.buffer.BufferPosition;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.util.Date;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.storage.mem.buffer.BufferPositionParser;

/**
 * An index that is used by the EventBuffer to store BinlogInfo->offset mappings.
 *
 * Main buffer is divided into regions (may contain multiple physical buffers).
 *
 * BinlogInfoIndex is a buffer too, which is divided into blocks. One block per region of the main buffer.
 * Each Block is of size SIZE_OF_BINLOGINFO_OFFSET_RECORD and contains BinlogInfo and corresponding OFFSET
 * (for the structure of the offset see BufferPosition.java)
 *
 * Each block describes one region in the main buffer. Number of regions in the main buffer is:
 * num_reigions = size_of_the_binlogInfo_index_buffer/SIZE_OF_BINLOGINFO_OFFSET_RECORD
 * Size of the region is: MainBufferSize/num_regions
 *
 * To find an BinlogInfo - one should look in BinlogInfo buffer to find the starting offset (using binary search)
 * in the main buffer and then scan sequentially starting from the offset.
 * Event_Window may span over multiple regions. In this case all the blocks in BinlogInfo index that correspond
 * to these regions will have same values (BinlogInfo and OFFSET of the start of the window).
 *
 * Next Event will start from the next available region (this is what is returned by getLargerOffset().
 *
 * In BinlogInfoIndex:
 * head - points to the next available block to read
 * tail - next available block to write
 *
 * Example:
 * Main buffer size - 1000bytes
 * BinlogInfo index size - 100bytes. 10 blocks.
 * 			 |---|----|---|----|---|-----|----|----|----|
 * binlogInfo| 4 | 5  | 5 | 5  | 6 | ....| 10 | 15 |    |
 * 	   offset| 0 | 100|100|100 |400| ....|700 |800 |    |
 *blockNumber|---|--- |---|----|---|-----|----|----|----|
 *           0   1   2    3              7    8    9
 *
 *
 * (binlogInfo, offset)
 * 			 |------|------|-------|-----|--------------------|-----------------|-----------|-----|
 * binlogInfo| 4    |   5  |  5    | 5   | 6 .......          |10,11,12,13,14   | 15        |     |
 *           |------|------|-------|-----|--------------------|-----------------|-----------|-----|
 *    offset 0      100    200     300   400                  700               800
 *
 * Event window with BinlogInfo 4 spans from offset 0 to 90.
 * Event window with BinlogInfo 5 spans from offset 100 to 350.
 * Event window with BinlogInfo 6 starts from offset 400.
 * The blocks 1,2,3 of BinlogInfo index will have the same BinlogInfo (5) and Offset(100). And block 4
 * will have BinlogInfo 6 and index 400.
 *
 * Events with BinlogInfo 10-14 are all fit into region between 700 and 800, so BinlogInfo index only points
 * at the first BinlogInfo of this range. So if one need to find event with BinlogInfo 12, they'll need
 * to do a serial scan from offset 700.
 */
public class BinlogInfoIndex extends InternalEventsListenerAbstract {
	public static final String MODULE = BinlogInfoIndex.class.getName();
	public static final Logger LOG = LoggerFactory.getLogger(MODULE);

	public static final String INDEX_METAINFO_FILE_NAME = "binlogInfoIndexMetaFile";
	public static final String INDEX_NAME = "binlogInfoIndex";
	private final ByteBuffer buffer;
	private final BufferPositionParser positionParser;
	private volatile int head = -1;  // head = first valid position in the index
	private volatile int tail = 0;  // tail = position at which we start writing
	private final int maxElements;
	private BinlogInfo lastBinlogInfoWritten;
	private int lastWrittenPosition = -1;
	private final int blockSize;
	private final int individualBufferSize;
	private final ReentrantReadWriteLock rwLock;
	private boolean updateOnNext;
	private boolean isFirstCheck = true;
	private final AssertLevel assertLevel;
	private final File mmapSessionDirectory;
	private final SourceType sourceType;
	private final short indexLength;

	public boolean isEnabled() {
		return enabled;
	}

	private final boolean enabled;

	private boolean updatedOnCurrentWindow = false; // see assertLastWrittenPosition

	private StorageConstant.IndexPolicy indexPolicy;

	/**
	 * Public constructor : takes in the number of index entries that will be kept
	 */
	public BinlogInfoIndex(int maxIndexSize, long totalAddressedSpace, int individualBufferSize,
			BufferPositionParser parser, EventBufferConstants.AllocationPolicy allocationPolicy, boolean restoreBuffer,
			File mmapSessionDirectory, AssertLevel assertLevel, boolean enabled, ByteOrder byteOrder,
			StorageConstant.IndexPolicy indexPolicy, SourceType sourceType) {
		Preconditions.checkArgument(allocationPolicy != EventBufferConstants.AllocationPolicy.MMAPPED_MEMORY, "Unsupported MMAP storage mode");
		Preconditions.checkArgument(assertLevel == AssertLevel.NONE, "Unsafe to assert the order of events");

		this.enabled = enabled;
		this.assertLevel = null != assertLevel ? assertLevel : AssertLevel.NONE;
		this.indexPolicy = indexPolicy;
		this.sourceType = sourceType;
		this.lastBinlogInfoWritten = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
		this.indexLength = (short)(BinlogInfoFactory.getLength(sourceType) + 8);
		rwLock = new ReentrantReadWriteLock();
		maxElements = maxIndexSize / indexLength;
		int proposedBlockSize = (int) (totalAddressedSpace / maxElements);
		if ((1L * proposedBlockSize * maxElements) < totalAddressedSpace) {
			proposedBlockSize++;
		}
		blockSize = proposedBlockSize;
		assert (1L * blockSize * maxElements >= totalAddressedSpace);

		positionParser = parser;
		int bufSize = maxElements * indexLength;

		String indexName = getIndexName();
		if (isEnabled()) {
			buffer = PtubesEventBuffer.allocateByteBuffer(bufSize, byteOrder, allocationPolicy, restoreBuffer,
					mmapSessionDirectory, new File(mmapSessionDirectory, indexName));
		} else {
			buffer = null;
		}

		this.mmapSessionDirectory = mmapSessionDirectory;

		this.individualBufferSize = individualBufferSize;

		updateOnNext = false;
		if (!isEnabled()) {
			LOG.info("BinlogInfoIndex not enabled");
			return;
		}

		LOG.info("BinlogInfoIndex configured with: maxElements = " + maxElements);
	}

	private String getIndexName() {
		return INDEX_NAME;
	}

	private String getMetaFileName() {
		return INDEX_METAINFO_FILE_NAME;
	}

	public void saveBufferMetaInfo() throws IOException {

		if (!isEnabled()) {
			return;
		}
		PtubesEventBufferMetaInfo mi = new PtubesEventBufferMetaInfo(
				new File(mmapSessionDirectory, getMetaFileName()));

		//save binlogInfoIndex file info
		mi.setBinlogInfoIndexBufferInfo(
				new PtubesEventBufferMetaInfo.BufferInfo(buffer.position(), buffer.limit(), buffer.capacity()));
		mi.setVal("scnBufferSize", Integer.toString(buffer.capacity()));

		mi.setVal("updateOnNext", Boolean.toString(updateOnNext));
		//private final  BufferPositionParser    positionParser;
		mi.setVal("head", Integer.toString(head));
		mi.setVal("tail", Integer.toString(tail));
		mi.setVal("maxElements", Integer.toString(maxElements));
		mi.setVal("lastBinlogInfoWritten", lastBinlogInfoWritten.toString());
		mi.setVal("lastWrittenPosition", Integer.toString(lastWrittenPosition));
		mi.setVal("blockSize", Integer.toString(blockSize));

		mi.setVal("individualBufferSize", Integer.toString(individualBufferSize));
		mi.setVal("isFirstCheck", Boolean.toString(isFirstCheck));
		mi.setVal("updatedOnCurrentWindow", Boolean.toString(updatedOnCurrentWindow));

		// AssertLevel assertLevel - no need to save, may be changed between the calles.
		// file mmapSessionDirectory; - no need to save, is passed as an arugment

		LOG.info("about to save binlogInfoindex state into " + mi.toString());
		mi.saveAndClose();
	}

	/**
	 * in case of MMaped file - flush the content
	 */
	@Override
	public void close() {
		if (isEnabled()) {
			flushMMappedBuffers();
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		if (isEnabled()) {
			sb.append("{\"head\": ");
			sb.append(head);
			sb.append(", \"headIdx\":");
			sb.append(head / indexLength);
			sb.append(", \"tail\":");
			sb.append(tail);
			sb.append(", \"tailIdx\":");
			sb.append(tail / indexLength);
			sb.append(", \"lastWrittenPosition\":");
			sb.append(lastWrittenPosition);
			sb.append(", \"lastBinlogInfoWritten\":");
			sb.append(lastBinlogInfoWritten);
			sb.append(", \"size\":");
			sb.append(size());
			sb.append(", \"maxSize\":");
			sb.append(maxElements);
			sb.append(", \"minScn\":");
			sb.append(getMinBinlogInfo());
			sb.append(", \"blockSize\":");
			sb.append(blockSize);
			sb.append(", \"individualBufferSize\":");
			sb.append(individualBufferSize);
			sb.append(", \"updateOnNext\":");
			sb.append(updateOnNext);
			sb.append(" \"assertLevel\":");
			sb.append(assertLevel);
			sb.append("}\n");
		} else {
			sb.append("{\"enabled\" : false}");
		}
		return sb.toString();
	}

	//
	// print each block's content from the index
	// if the same offset - consolidate into one line
	// also (to save disk space and improve readability) - use simplified log format
	public void printVerboseString() {
		LOG.error(toString());
		if (!isEnabled()) {
			LOG.error("BinlogInfoIndex is disabled");
			return;
		}

		long currentOffset, beginBlock;
		long endBlock;
		BinlogInfo currentBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
		currentOffset = -1;
		endBlock = beginBlock = 0;
		LOG.error("logger name: " + LOG.getName() + " ts:" + (new Date()));
		for (int position = 0; position < buffer.limit(); position += indexLength) {
			long nextOffset = getOffset(position);
			if (currentOffset < 0) {
                currentOffset = nextOffset;
            }
			long nextBlock = position / indexLength;
			BinlogInfo nextBinlogInfo = getBinlogInfo(position);
			if (!currentBinlogInfo.isValid()) {
				currentBinlogInfo = nextBinlogInfo;
			}
			if (nextOffset != currentOffset || !currentBinlogInfo.isEqualTo(nextBinlogInfo, indexPolicy)) {
				// new offset - print the previous line
				LOG.error(buildBlockIndexString(beginBlock, endBlock, currentBinlogInfo, currentOffset));

				currentOffset = nextOffset;
				beginBlock = nextBlock;
				currentBinlogInfo = nextBinlogInfo;
			}

			endBlock = nextBlock;
		}
		// final line
		LOG.error(buildBlockIndexString(beginBlock, endBlock, currentBinlogInfo, currentOffset));
	}

	private String buildBlockIndexString(long beginBlock, long endBlock, BinlogInfo binlogInfo, long offset) {
		String block = "" + endBlock;
		if (beginBlock != endBlock) {
            block = "[" + beginBlock + "-" + endBlock + "]";
        }
		return block + ":" + binlogInfo + "->" + offset + " " + positionParser.toString(offset);
	}

	public BinlogInfo getMinBinlogInfo() {
		if (!isEnabled()) {
			throw new RuntimeException("BinlogInfoIndex not enabled");
		}
		acquireReadLock();
		try {
			if (empty()) {
				return BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
			} else {
				BinlogInfo minBinlogInfo = getBinlogInfo(head);
				return minBinlogInfo;
			}
		} finally {
			releaseReadLock();
		}
	}

	private void acquireReadLock() {
		rwLock.readLock().lock();
	}

	private void releaseReadLock() {
		rwLock.readLock().unlock();
	}

	public void acquireWriteLock() {
		rwLock.writeLock().lock();
	}

	public void releaseWriteLock() {
		rwLock.writeLock().unlock();
	}

	private void setBinlogInfoOffset(int blockNumber, BinlogInfo binlogInfo, long offset) {
		boolean isDebug = LOG.isDebugEnabled();
		assertEquals("BinlogInfo Index Tail Check", buffer.position(), tail, AssertLevel.QUICK);

		int startPos = tail;

		updatedOnCurrentWindow = false;

		// if new window in the same block we just wrote, skip :
		// In the case of multiple window boundaries insied a single block : BinlogInfoIndex always points to the first window in that block
		if (lastWrittenPosition == blockNumber * indexLength) {
			return;
		}

		try {
			acquireWriteLock();

			int currentWritePosition = tail;
			boolean overwritingHead = (currentWritePosition == head);

			if (lastWrittenPosition >= 0) {
				BinlogInfo lastWrittenBinlogInfo = this.getBinlogInfo(lastWrittenPosition);
				long lastWrittenOffset = this.getOffset(lastWrittenPosition);

				// Setup the blocks between tail and the passed block number.
				while ((!overwritingHead) && (currentWritePosition != blockNumber * indexLength)) {
					if (currentWritePosition == head) {
						overwritingHead = true;
						break;
					}

					if (isDebug) {
						LOG.debug(
							"BinlogInfoIndex:Extend:" + "[" + buffer.position() + "]" + lastWrittenBinlogInfo + "->"
								+ lastWrittenOffset);
					}

					buffer.put(lastWrittenBinlogInfo.encode());
					buffer.putLong(lastWrittenOffset);

					lastWrittenPosition = currentWritePosition;

					if (buffer.position() == buffer.limit()) {
						buffer.position(0);
					}

					currentWritePosition = buffer.position();
				}

				if (currentWritePosition == head) {
					overwritingHead = true;
				}
			} else {
				overwritingHead = false;
			}

			//Update the corresponding index entry only when we are not overwriting HEAD.
			if (!overwritingHead) {
				if (isDebug) {
					LOG.debug("BinlogInfoIndex:Write:" + "[" + buffer.position() + "]" + binlogInfo + "->" + offset);
				}

				buffer.put(binlogInfo.encode());
				buffer.putLong(offset);
				lastBinlogInfoWritten = binlogInfo;
				lastWrittenPosition = currentWritePosition;
				updatedOnCurrentWindow = true;

				if (buffer.position() == buffer.limit()) {
					buffer.position(0);
				}

				currentWritePosition = buffer.position();
			}

			if (head < 0) {
				// Buffer was empty, we should have written starting from startPosition
				head = startPos;
			}

			// Set tail to be ready for the next write
			tail = buffer.position();

			if (isDebug) {
				LOG.debug("Setting BinlogInfoIndex tail to :" + tail + " after writing EVB offset :" + positionParser
						.toString(offset) + " for BinlogInfo : " + binlogInfo + " till block number :" + blockNumber);
			}

			// Attention: never enter into this block of codes - pyf
			if (assertLevel.quickEnabled()) {
				assertHead();
				assertTail();

				if (assertLevel.mediumEnabled()) {
					assertOrder();
					assertOffsets();
				}
			}
		} finally {
			releaseWriteLock();
		}
	}

	public int getBlockNumber(long offset) {
		if (!isEnabled()) {
			throw new RuntimeException("BinlogInfoIndex not enabled");
		}
		long bufferIndex = positionParser.bufferIndex(offset);
		long buf_offset = positionParser.bufferOffset(offset);
		long blockNumber = (bufferIndex * this.individualBufferSize + buf_offset) / blockSize;
		return (int) blockNumber;
	}

	public void flushMMappedBuffers() {
		if (!isEnabled()) {
			return;
		}
		if (buffer instanceof MappedByteBuffer) {
			((MappedByteBuffer) buffer).force();
		}
	}

	/*
	 * BinlogInfoIndexEntry
	 *
	 * Wraps the contents of each record (BinlogInfo, offset)
	 * Primarily used for passing this info to the caller
	 */
	public class BinlogInfoIndexEntry {
		private final SourceType sourceType;
		private byte[] binlogInfo;
		private long offset;

		public BinlogInfo getBinlogInfo() {
			return BinlogInfoFactory.decode(sourceType, binlogInfo);
		}

		public void setBinlogInfo(BinlogInfo binlogInfo) {
			this.binlogInfo = binlogInfo.encode();
		}

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		public BinlogInfoIndexEntry(BinlogInfo binlogInfo, long offset) {
			super();
			this.sourceType = binlogInfo.getSourceType();
			this.binlogInfo = binlogInfo.encode();
			this.offset = offset;
		}
	}

	/**
	 * get an BinlogInfoIndex which is less than or equal to searchBinlogInfo
	 * @param searchBinlogInfo
	 * @return max{BinlogInfoIndex}, BinlogInfoIndex <= searchBinlogInfo
	 * @throws OffsetNotFoundException
	 */
	// for mysql
	public BinlogInfoIndexEntry getClosestOffset(BinlogInfo searchBinlogInfo) throws OffsetNotFoundException {
		acquireReadLock();
		try {
			if (!isEnabled()) {
				throw new RuntimeException("BinlogInfoIndex not enabled");
			}
			if (empty()) {
				LOG.info("BinlogInfoIndex is empty");
				throw new OffsetNotFoundException();
			}
			// binary search
			final int startRightOfs = tail;
			int left = head;
			int right = startRightOfs;
			int index;
			index = midPoint(left, right, buffer.limit());

			BinlogInfo currBinlogInfo = getBinlogInfo(index);
			boolean found = false;
			while (!found) {
				if (isClosestBinloginfo(currBinlogInfo, searchBinlogInfo, index, startRightOfs)) {
					int lessIndex = decrement(index, buffer.limit());
					BinlogInfo lessBinlogInfo = getBinlogInfo(lessIndex);
					while ((index != head) && (currBinlogInfo == lessBinlogInfo)) {
						index = lessIndex;
						currBinlogInfo = lessBinlogInfo;
						lessIndex = decrement(index, buffer.limit());
						lessBinlogInfo = getBinlogInfo(lessIndex);
					}
					break;
				} else {
					if (currBinlogInfo.isGreaterThan(searchBinlogInfo, indexPolicy)) {
						if ((index == right) || ((left + indexLength) % buffer.limit() == right)) {
							LOG.error(
								"Case 1 : currBinlogInfo > searchBinlogInfo and index == right" + "index = " + index
									+ " right = " + right + " left = " + left + " buffer.limit = " + buffer
									.limit() + " searchBinlogInfo = " + searchBinlogInfo + " currBinlogInfo = "
									+ currBinlogInfo);
							printVerboseString();
							throw new OffsetNotFoundException();
						}
						right = index;
					} else {
						if (index == left) {
							LOG.error(
								"Case 2 : currBinlogInfo <= searchBinlogInfo and index == left" + "index = " + index
									+ " right = " + right + " left = " + left + " buffer.limit = " + buffer
									.limit() + " searchBinlogInfo = " + searchBinlogInfo + " currBinlogInfo = "
									+ currBinlogInfo);
							printVerboseString();
							throw new OffsetNotFoundException();
						}
						left = index;
					}
					int prevIndex = index;
					index = midPoint(left, right, buffer.limit());
					if (prevIndex == index) {
						LOG.error("Case 3 : currBinlogInfo > searchBinlogInfo and prevIndex == index" + "index = " + index
							+ " prevIndex = " + prevIndex + " right = " + right + " left = " + left
							+ " buffer.limit = " + buffer.limit() + " searchBinlogInfo = " + searchBinlogInfo
							+ " currBinlogInfo = " + currBinlogInfo);
						printVerboseString();
						throw new OffsetNotFoundException();
					}
					currBinlogInfo = getBinlogInfo(index);
				}
			}
			return new BinlogInfoIndexEntry(currBinlogInfo, getOffset(index));
		} finally {
			releaseReadLock();
		}
	}

	private boolean empty() {
		return (numElements(head, tail, buffer.limit()) == 0);
	}

	/**
	 * Returns the binlogInfo of an index entry
	 *
	 * @param entryOffset
	 * 		the physical offset of the entry in the index buffer
	 */
	private BinlogInfo getBinlogInfo(int entryOffset) {
		int binlogInfoLength = BinlogInfoFactory.getLength(sourceType);
		byte[] binlogInfoByte = new byte[binlogInfoLength];
		for (int i = 0; i < binlogInfoLength; i++) {
			binlogInfoByte[i] = buffer.get(entryOffset + i);
		}
		return BinlogInfoFactory.decode(sourceType, binlogInfoByte);
	}

	/**
	 * Returns the offset of an index entry
	 *
	 * @param entryOffset
	 * 		the physical offset of the entry in the index buffer
	 */
	private long getOffset(int entryOffset) {
		return buffer.getLong(entryOffset + BinlogInfoFactory.getLength(sourceType));
	}

	private int entryIndexOfs(int entryIndex) {
		return (head + entryIndex * indexLength) % (indexLength
				* maxElements);
	}

	/**
	 * Returns the binlogInfo of an index entry
	 *
	 * @param entryIndex
	 * 		the index of the entry relative to the head
	 * @return the binlogInfo or -1 if not found
	 */
	private BinlogInfo getEntryBinlogInfo(int entryIndex) {
		if (-1 == head) {
			return BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
		}
		return getBinlogInfo(entryIndexOfs(entryIndex));
	}

	/**
	 * Returns the offset of an index entry
	 *
	 * @param entryIndex
	 * 		the index of the entry relative to the head
	 * @return the binlogInfo or -1 if not found
	 */
	private long getEntryOffset(int entryIndex) {
		if (-1 == head) {
            return -1;
        }
		return getOffset(entryIndexOfs(entryIndex));
	}

	// please treat indexBinlogInfo as currBinlogInfo
	// When searchBinlogInfo > indexBinlogInfo or not comparable, it should be addressed one by one
	// scene 1: searchBinlogInfo.isGreaterThan(indexBinlogInfo) only means searchBinlogInfo > indexBinlogInfo
	// scene 2: indexBinlogInfo.isGreaterThan(searchBinlogInfo) == false means indexBinlogInfo < searchBinlogInfo and not comparable
	private boolean isClosestBinloginfo(BinlogInfo indexBinlogInfo, BinlogInfo searchBinlogInfo, int index, int maxIndex) {
		if (indexBinlogInfo.isEqualTo(searchBinlogInfo, indexPolicy)) {
			return true;
		}
		if (indexBinlogInfo.isGreaterThan(searchBinlogInfo, indexPolicy) == false) {
			if ((index + indexLength) % buffer.limit() == maxIndex) {
				// reach the border
				return true;
			} else {
				// Consistent with scene2
				BinlogInfo rightIndexBinlogInfo = getBinlogInfo((index + indexLength) % buffer.limit());
				return searchBinlogInfo.isGreaterThan(rightIndexBinlogInfo, indexPolicy) == false;
			}
		}
		return false;
	}

	private int size() {
		return numElements(head, tail, maxElements * indexLength);
	}

	private int numElements(int left, int right, int end) {
		if (left < 0) {
			return 0;
		}
		if (left == right) {
			return end / indexLength;
		}
		if (left < right) {
			return ((right - left) / indexLength);
		} else {
			return (end - left + right) / indexLength;
		}
	}

	private int midPoint(int left, int right, int end) {
		int size = numElements(left, right, end);
		return (left + (size / 2) * indexLength) % end;
	}

	private int decrement(int index, int end) {
		if (index >= indexLength) {
			return (index - indexLength);
		} else {
			return (end - indexLength);
		}
	}

	public void clear() {
		if (!isEnabled()) {
			return;
		}
		buffer.rewind();
		head = -1;
		tail = 0;
		this.lastBinlogInfoWritten = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
		lastWrittenPosition = head;
		updateOnNext = false;

	}

	@Override
	public void onEvent(PtubesEvent event, long offset, int size) {
		if (!isEnabled()) {
			return;
		}
		boolean shouldUpdate = shouldUpdate(event);
		if (LOG.isDebugEnabled()) {
			LOG.debug(
				"BinlogInfoIndex:onEvent:offset" + offset + " EventBinlogInfo:" + event.getBinlogInfo() + ";shouldUpdate="
					+ shouldUpdate);
		}

		if (shouldUpdate) {
			// event window binlogInfo should be monotonically increasing
			BinlogInfo binlogInfo = event.getBinlogInfo();
			if (lastBinlogInfoWritten.isGreaterThan(binlogInfo, indexPolicy)) {
				throw new RuntimeException(
						"Event BinlgInfo " + binlogInfo + " < lastBinlogInfoWritten " + lastBinlogInfoWritten);
			}
			int blockNumber = getBlockNumber(offset);
			setBinlogInfoOffset(blockNumber, binlogInfo, offset);
		}
	}

	@Override
	public void onTxn(BinlogInfo binlogInfo, long offset) {
		if (!isEnabled()) {
			return;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("BinlogInfoIndex:onEvent:offset" + offset + " EventBinlogInfo:" + binlogInfo);
		}

		if (validateAppendable(binlogInfo) == false/*lastBinlogInfoWritten.isGreaterThan(binlogInfo, indexPolicy)*/) {
			throw new RuntimeException(
					"Event BinlgInfo " + binlogInfo + " < lastBinlogInfoWritten " + lastBinlogInfoWritten);
		}
		int blockNumber = getBlockNumber(offset);
		setBinlogInfoOffset(blockNumber, binlogInfo, offset);
	}

	private boolean validateAppendable(BinlogInfo eventBinlogInfo) {
		switch (sourceType) {
			case MySQL:
				return lastBinlogInfoWritten.isGreaterThan(eventBinlogInfo, indexPolicy) == false;
			default:
				throw new UnsupportedOperationException("Unsupported binlog info comparison in source type " + sourceType.name());
		}
	}

	private boolean shouldUpdate(PtubesEvent event) {
		return true;
	}

	public boolean isEmpty() {
		if (!isEnabled()) {
			throw new RuntimeException("BinlogInfoIndex not enabled");
		}
		return head == -1;
	}

	public void setUpdateOnNext(boolean val) {
		if (!isEnabled()) {
			return;
		}
		updateOnNext = val;
	}

	/**
	 * Return a valid offset (which represents a valid event window start point) which is larger than the offset passed in
	 * If there are no valid offsets then return -1
	 *
	 * @param offset
	 */
	public long getLargerOffset(long offset) {
		if (!isEnabled()) {
			throw new RuntimeException("BinlogInfoIndex not enabled");
		}
		// Note: not acquiring read lock because this is called by the writer
		boolean trace = false;
		int blockNumber = getBlockNumber(offset);
		long currentOffset = getOffset(blockNumber * indexLength);
		// the retrieved offset could be < , == or > than the passed in offset .. haha !
		int maxIterations = numElements(head, tail, buffer.limit());

		if (trace) {
			LOG.info("offset = " + offset + ";blockNumber = " + blockNumber + ";currentOffset = " + currentOffset
					+ ";currentBinlogInfo = " + getBinlogInfo(blockNumber * indexLength));
			LOG.info("maxIterations = " + maxIterations);
		}
		long prevOffset = currentOffset;
		while ((prevOffset == currentOffset) && (maxIterations > 0)) {
			blockNumber++;
			if (blockNumber >= maxElements) {
				blockNumber = 0;
			}
			prevOffset = currentOffset;
			currentOffset = getOffset(blockNumber * indexLength);
			--maxIterations;
		}

		// either currentOffset is > offset or it is wrapped around or we couldn't find a valid candidate
		if (maxIterations == 0) {
			return -1;
		}

		if (trace) {
			LOG.info("Returning currentOffset = " + currentOffset + " prevOffset = " + prevOffset + " offset = "
					+ offset);
		}
		return currentOffset;
	}

	public void moveHead(long offset) {
		if (!isEnabled()) {
			return;
		}
		moveHead(offset, BinlogInfoFactory.newDefaultBinlogInfo(sourceType));
	}

	/**
	 * This method is to get around a problem with the BinlogInfoIndex when an Iterator is trying to
	 * remove events at the head of a window. If the window is pointed by entries in the BinlogInfoIndex
	 * after the removal, these entries will become invalid.
	 *
	 * THIS IS A HACK! After this look ups in the index will return partial results for this window.
	 * Luckily currently the iterator removal is used only by the client which does not use the
	 * BinlogInfoIndex.
	 *
	 * @param newHeadOffset
	 * 		the new event buffer head
	 */
	public void moveHead(long newHeadOffset, BinlogInfo newHeadBinlogInfo) {
		if (!isEnabled()) {
			return;
		}
		boolean debugEnabled = LOG.isDebugEnabled();

		int proposedHead;
		int blockNumber = -1;
		if (newHeadOffset < 0) {
			if (head >= 0) {
				LOG.warn("track(BinlogInfoIndex.head): resetting head to -1: " + newHeadOffset);
			}
			proposedHead = -1;
		} else {
			blockNumber = getBlockNumber(newHeadOffset);
			proposedHead = blockNumber * indexLength;
		}

		acquireWriteLock();
		try {
			//we have to be careful about the following situations:
			// |---H---T---PH----| or |---T--PH----H---|
			// The above can happen if we are on the client side, and start removing events from a window
			// before we've seen the end of the window. In that case, the tail has not been updated yet
			// and the proposed head can overshoot the tail.
			boolean moveTail =
					newHeadBinlogInfo.isValid() && (head < tail && tail <= proposedHead) || (proposedHead >= tail
							&& proposedHead < head);

			if (moveTail && blockNumber >= 0) {
				BinlogInfo oldHeadBinlogInfo = head >= 0 ? getBinlogInfo(head) : BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
				long oldHeadOfs = head >= 0 ? getOffset(head) : -1;

				if (0 == proposedHead && head > 0) {
					LOG.info("in:" + newHeadBinlogInfo);
				}

				//Update the tail to match the new head position
				if (debugEnabled) {
					LOG.debug("adjusting tail to match net head position; before: proposedHead=" + proposedHead
							+ " newOfs=" + newHeadOffset + " newBinlogInfo=" + newHeadBinlogInfo + " oldBinlogInfo="
							+ oldHeadBinlogInfo + " oldOfs=" + oldHeadOfs);
					LOG.debug(this.toString());
				}
				setBinlogInfoOffset(blockNumber, newHeadBinlogInfo,
						oldHeadBinlogInfo.isEqualTo(newHeadBinlogInfo, indexPolicy) ? oldHeadOfs : newHeadOffset);
				if (debugEnabled) {
					LOG.debug("adjusting tail to match net head position; after " + this);
				}
			}

			head = proposedHead;
			if (LOG.isDebugEnabled()) {
				LOG.debug("After move head: " + this);
			}

			if (head >= 0) {
				long oldHeadOfs = getOffset(head);
				if (oldHeadOfs != newHeadOffset) {
					//we need to update the Offset for all adjacent entries with the same offset as the head
					int i = head;
					int n = size();
					long curOfs = oldHeadOfs;
					do {
						buffer.putLong(i + 8, newHeadOffset);
						i += indexLength;
						if (buffer.limit() - i < indexLength) {
							i = 0;
						}
						n--;

						if (n > 0) {
							curOfs = getOffset(i);
						}
					} while (i != tail && n > 0 && curOfs == oldHeadOfs);
				}
			} else {
				tail = 0;
			}

			if (assertLevel.quickEnabled()) {
				assertHead();
				assertTail();
			}

		} finally {
			releaseWriteLock();
		}
	}

	/*
	 * Non-Junit Utility method for assertions.
	 */
	private void assertEquals(String message, long exp, long actual, AssertLevel level) {
		if (assertLevel.getIntValue() < level.getIntValue()) {
			return;
		}

		if (exp != actual) {
			LOG.error("FAIL :" + message + ", Expected :" + exp + ", Actual :" + actual);
			throw new RuntimeException("FAIL :" + message + ", Expected :" + exp + ", Actual :" + actual);
		}
	}

	/*
	 * Asserts that EVB Head and BinlogInfoIndex Head is consistent
	 */
	public void assertHeadPosition(long evbHead) {
		if (!isEnabled()) {
			return;
		}
		long expHead = -1;
		if (evbHead > 0) {
			int blockNumber = getBlockNumber(evbHead);
			expHead = blockNumber * indexLength;

			if (expHead != head) {
				String msg = "BinlogInfo Index Head is (" + head + ") Expected Head was (" + expHead
						+ ") Event Buffer Head is :" + evbHead;
				LOG.error(msg);
				LOG.error("BinlogInfo Index is (assertHeadPosition):");
				printVerboseString();
				throw new RuntimeException(msg.toString());
			}
		}
	}

	/*
	 * Asserts that EVB eventStartIndex and BinlogInfoIndex lastWrittenPosition is consistent
	 * This is a consistency check called in endEvents after the BinlogInfoIndex is updated.
	 * eventStartIndex is the EVB offset to the beginning of the current window (whose endEvents() is being processed)
	 * lastWrittenPosition refers to the last BinlogInfoIndex block we updated.
	 *
	 *   Except for the following cases, both of these values should be equal
	 *   a) First Event Window written to the EVB
	 *   b) when overWritingHead was detected while trying to update the BinlogInfoIndex for the current window.
	 */
	public void assertLastWrittenPos(BufferPosition evbStartPos) {
		if (!isEnabled()) {
			return;
		}
		long expTail = -1;
		long evbStartLoc = evbStartPos.getRealPosition();

		if ((evbStartPos.getPosition() > 0) && (!isFirstCheck) && updatedOnCurrentWindow) {
			int blockNumber = getBlockNumber(evbStartLoc);
			expTail = blockNumber * indexLength;

			if (expTail != lastWrittenPosition) {
				StringBuilder msg = new StringBuilder();
				msg.append("BinlogInfo Index LWP is (" + lastWrittenPosition + ") Expected LWP was (" + expTail
						+ ") Event Buffer Start Index is :" + evbStartPos);
				msg.append("BinlogInfo Index is(assertLastWrittenPos) :" + toString());
				msg.append("BinlogInfo at LWP: " + getBinlogInfo(lastWrittenPosition) + ", Offset :" + positionParser
						.toString(getOffset(lastWrittenPosition)));
				msg.append("evbStartLoc:" + evbStartLoc).append(
						"positionParser.bufferIndex(offset):" + positionParser.bufferIndex(evbStartLoc)).append(
						"positionParser.bufferOffset(offset):" + positionParser.bufferOffset(evbStartLoc));

				LOG.error(msg.toString());
				throw new RuntimeException(msg.toString());
			}
		}
		isFirstCheck = false;
	}

	private void smartVerbosePrint() {
		if (LOG.isDebugEnabled() || maxElements < 10000) {
			printVerboseString();
		}
	}

	private void assertTail() {
		if (empty()) {
			if (0 != tail) {
                throw new AssertionError("invalid empty tail: " + tail + "; state: " + this);
            }
		} else if (0 > tail || maxElements * indexLength <= tail) {
			throw new AssertionError("invalid tail: " + tail + "; state: " + this);
		} else if (0 != tail % indexLength) {
			throw new AssertionError("tail not aligned: " + tail + "; state: " + this);
		}
	}

	private void assertHead() {
		if (empty()) {
			if (head != -1) {
                throw new AssertionError("invalid empty head: " + head + "; state: " + this);
            }
		} else {
			if (0 != head % indexLength) {
				throw new AssertionError("head not aligned: " + head + "; state: " + this);
			}

			if (0 > head || maxElements * indexLength <= head) {
				throw new AssertionError("invalid head: " + head + "; state: " + this);
			}
		}
	}

	private void assertOrder() {
		int sz = size();
		if (0 == sz) {
            return;
        }

		for (int i = 0; i < sz - 1; ++i) {
			BinlogInfo binlogInfo1 = getEntryBinlogInfo(i);
			BinlogInfo binlogInfo2 = getEntryBinlogInfo(i + 1);
			long ofs1 = getEntryOffset(i);
			long ofs2 = getEntryOffset(i + 1);

			if (binlogInfo1.isGreaterThan(binlogInfo2, indexPolicy)) {
				smartVerbosePrint();
				throw new AssertionError(
						"binlogInfo[" + i + "] = " + binlogInfo1 + " > " + binlogInfo2 + " = binlogInfo[" + (i + 1)
								+ "]; \nstate:" + this);
			} else if (binlogInfo1.equals(binlogInfo2)) {
				if (ofs1 != ofs2) {
					smartVerbosePrint();
					throw new AssertionError(
							"binlogInfo[" + i + "] = " + binlogInfo1 + " == " + binlogInfo2 + " = binlogInfo[" + (i + 1)
									+ "] but ofs[" + i + "] = " + ofs1 + "!= " + ofs2 + " = ofs[" + (i + 1)
									+ "]; \nstate:" + this);
				}
			} else if (ofs1 >= ofs2) {
				smartVerbosePrint();
				throw new AssertionError(
						"ofs[" + i + "] = " + ofs1 + " >= " + ofs2 + " = ofs[" + (i + 1) + "] but binlogInfo[" + i
								+ "] = " + binlogInfo1 + " < " + binlogInfo2 + " = binlogInfo[" + (i + 1)
								+ "]; \nstate:" + this);
			}
		}
	}

	private void assertOffsets() {
		int sz = size();
		if (0 == sz) {
            return;
        }

		long minGen = Long.MAX_VALUE;
		long maxGen = Long.MIN_VALUE;
		int minGenIdx = -1;
		int maxGenIdx = -1;
		long minOfs = -1;
		long maxOfs = 2;

		int lastNewOfs = -1;
		int prevOfs = -1;
		for (int i = 0; i < sz; ++i) {
			int indexOfs = entryIndexOfs(i); //physical offset in the index of the i-th entry

			long ofs1 = getOffset(indexOfs); //offset in the event buffer for the i-th entry
			long ofs_1 = prevOfs != -1 ? getOffset(prevOfs) : -1; //offset in the event buffer for the (i-1)-th entry

			long gen1 = positionParser.bufferGenId(ofs1);

			long block1 = getBlockNumber(ofs1);
			int expBlock = -1;

			if (i == 0 || ofs1 != ofs_1) {
                expBlock = indexOfs / indexLength;
            } else {
                expBlock = lastNewOfs / indexLength;
            }

			if (gen1 < minGen) {
				minGen = gen1;
				minGenIdx = i;
				minOfs = ofs1;
			}
			if (gen1 > maxGen) {
				maxGen = gen1;
				maxGenIdx = i;
				maxOfs = ofs1;
			}

			if (expBlock != block1) {
				smartVerbosePrint();
				throw new AssertionError(
						"block(offset[" + i + "]=" + ofs1 + ")=" + block1 + " != " + expBlock + "; state:" + this);
			}

			if (ofs1 != ofs_1) {
				lastNewOfs = indexOfs;
			}
			prevOfs = indexOfs;
		}

		if (maxGen - minGen > 1) {
			smartVerbosePrint();
			throw new AssertionError(
					"genId (ofs[" + minGenIdx + "]) + 1 = " + (minGen + 1) + " < " + maxGen + " = genId(ofs[ "
							+ maxGenIdx + "]); minOfs=" + minOfs + "->" + positionParser.toString(minOfs) + "; maxOfs="
							+ maxOfs + "->" + positionParser.toString(maxOfs) + "; \nstate:" + this);
		}

	}

	/**
	 * package private to allow helper classes to inspect internal details
	 */
	int getHead() {
		if (!isEnabled()) {
			throw new RuntimeException("BinlogInfoIndex not enabled");
		}
		return head;
	}

	/**
	 * package private to allow helper classes to inspect internal details
	 */
	int getTail() {
		if (!isEnabled()) {
			throw new RuntimeException("BinlogInfoIndex not enabled");
		}
		return tail;
	}

	/** The position parser used byt the index (for testing purposes) */
	BufferPositionParser getPositionParser() {
		if (!isEnabled()) {
			throw new RuntimeException("BinlogInfoIndex not enabled");
		}
		return positionParser;
	}
}
