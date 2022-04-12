package com.meituan.ptubes.reader.storage.mem;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.GreaterThanStorageRangeException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.common.exception.OffsetNotFoundException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.storage.common.event.EventInternalWritable;
import com.meituan.ptubes.reader.storage.mem.buffer.BinlogInfoFactory;
import com.meituan.ptubes.reader.storage.mem.buffer.PtubesEventBuffer;
import com.meituan.ptubes.reader.storage.mem.buffer.constants.EventScanningState;
import com.meituan.ptubes.reader.storage.mem.buffer.index.BinlogInfoIndex;

import static com.meituan.ptubes.reader.storage.mem.buffer.constants.EventScanningState.FOUND_WINDOW_ZONE;


public class MemStorage extends PtubesEventBuffer {
	private final SourceType sourceType;
	private final StorageConfig storageConfig;

	public MemStorage(StorageConfig config, SourceType sourceType) {
		super(config.getMemConfig(), config.getReaderTaskName(), config.getIndexPolicy(), sourceType);
		this.sourceType = sourceType;
		this.storageConfig = config;
	}

	// The tentatively added interfaces are as follows, others are the same as DbusEventBuffer

	public PtubesEventIterator getOldestInternalIterator(String iteratorName) {
		acquireWriteLock();
		try {
			PtubesEventIterator eventIterator = acquireIterator(head.getPosition(), tail.getPosition(), iteratorName);
			return eventIterator;
		} finally {
			releaseWriteLock();
		}
	}

	public PtubesEventIterator getNormalInternalIterator(BinlogInfo binlogInfo, String iteratorName)
		throws LessThanStorageRangeException, GreaterThanStorageRangeException{
		switch (sourceType) {
			case MySQL:
				return getInternalIterator(binlogInfo, iteratorName);
			default:
				throw new PtubesRunTimeException("Unsupported getting iterator for source type " + sourceType.name());
		}
	}

	public PtubesEventIterator getLatestInternalIterator(String iteratorName)
			throws LessThanStorageRangeException, GreaterThanStorageRangeException {
		BinlogInfo binlogInfo = getLastWrittenBinlogInfo();
		switch (sourceType) {
			case MySQL:
				return getInternalIterator(binlogInfo, iteratorName);
			default:
				throw new PtubesRunTimeException("Unsupported getting iterator for source type " + sourceType.name());
		}
	}

	/**
	 * work for MySQL BinlogInfoIndex,
	 * @param binlogInfo
	 * @param iteratorName
	 * @return
	 * @throws LessThanStorageRangeException
	 * @throws GreaterThanStorageRangeException
	 */
	public PtubesEventIterator getInternalIterator(BinlogInfo binlogInfo, String iteratorName)
		throws LessThanStorageRangeException, GreaterThanStorageRangeException {
		StorageConstant.IndexPolicy indexPolicy = storageConfig.getIndexPolicy();
		BinlogInfo minBinlogInfo = getMinBinlogInfo();
		BinlogInfo lastWrittenBinlogInfo = getLastWrittenBinlogInfo();

		if (minBinlogInfo.isGreaterThan(
			binlogInfo,
			indexPolicy
		)) {
			log.error("request binlogInfo(" + binlogInfo.toString() + ") is less than minBinlogInfo(" + minBinlogInfo
				.toString() + ")");
			throw new LessThanStorageRangeException(
				"request binlogInfo(" + binlogInfo.toString() + ") is less than minBinlogInfo(" + minBinlogInfo
					.toString() + ")");
		}

		if (binlogInfo.isGreaterThan(
			lastWrittenBinlogInfo,
			indexPolicy
		)) {
			log.error("request binlogInfo(" + binlogInfo.toString() + ") is greater than maxBinlogInfo(" +
				lastWrittenBinlogInfo
					.toString() + ")");
			throw new GreaterThanStorageRangeException(
				"request binlogInfo(" + binlogInfo.toString() + ") is mogreaterre than lastWrittenBinlogInfo(" +
					lastWrittenBinlogInfo
						.toString() + ")");
		}

		BinlogInfoIndex.BinlogInfoIndexEntry binlogInfoIndexEntry = null;
		try {
			binlogInfoIndexEntry = binlogInfoIndex.getClosestOffset(binlogInfo);
			log.info("searchBinlogInfo: {}, binlogInfoIndex: {}", binlogInfo.toString(), binlogInfoIndexEntry.getBinlogInfo().toString());
		} catch (OffsetNotFoundException e) {
			log.error("Get closest offset from index error", e);
			throw new PtubesRunTimeException(e);
		}
		long offset = binlogInfoIndexEntry.getOffset();
		
		BinlogInfo indexEntryBinlogInfo = binlogInfoIndexEntry.getBinlogInfo();
		PtubesEventIterator eventIterator = acquireIterator(offset,
			bufferPositionParser.sanitize(tail.getPosition(), buffers), iteratorName);

		// find some
		EventScanningState state = EventScanningState.LOOKING_FOR_FIRST_VALID_EVENT;
		boolean isFirstEvent = true;
		BinlogInfo currentWindowBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);
		BinlogInfo prevWindowBinlogInfo = BinlogInfoFactory.newDefaultBinlogInfo(sourceType);

		while (eventIterator.hasNext()) {
			EventInternalWritable e;
			try {
				e = eventIterator.next(isFirstEvent);
				if (isFirstEvent) {
					BinlogInfo eventBinlogInfo = e.getBinlogInfo();
					LOG.info("event: {}", e.toString());
					if (!indexEntryBinlogInfo.isEqualTo(eventBinlogInfo, indexPolicy)) {
						String msg = "Concurrent Overwritting of Event. Expected sequence :" + indexEntryBinlogInfo
							+ ", Got event=" + e.toString();
						log.warn(msg);
						throw new PtubesRunTimeException(msg);
					}
				}
			} catch (Exception excep) {
				log.warn("Found invalid event on getting iterator. This is not unexpected but should be investigated.");
				log.warn("RangeBasedLocking :" + rwLockProvider.toString(bufferPositionParser, true));
				throw new PtubesRunTimeException(excep);
			}

			isFirstEvent = false;
			BinlogInfo eventBinlogInfo = e.getBinlogInfo();

			//address the first event
			if (state == EventScanningState.LOOKING_FOR_FIRST_VALID_EVENT) {
				if (eventBinlogInfo.isGreaterThan(binlogInfo, indexPolicy)) {
					// eventBinlogInfo > binlogInfo
					log.info("BinlogInfoIndex state = " + binlogInfoIndex);
					log.info("Iterator = " + eventIterator);
					log.info("Found event " + e + " while looking for binlogInfo = " + binlogInfo);
					throw new PtubesRunTimeException(
						"Found event " + e + " while looking for binlogInfo = " + binlogInfo);
				} else {
					// eventBinlogInfo <= binlogInfo
					state = EventScanningState.IN_LESS_THAN_EQUALS_BINLOGINFO_ZONE;
				}
			}

			// In an interval smaller than the binlogInfo that needs to be addressed
			if (state == EventScanningState.IN_LESS_THAN_EQUALS_BINLOGINFO_ZONE) {
				if (binlogInfo.isGreaterThan(eventBinlogInfo, indexPolicy)) {
					currentWindowBinlogInfo = eventBinlogInfo;
					continue;
				} else if (binlogInfo.isEqualTo(eventBinlogInfo, indexPolicy)) {
					state = FOUND_WINDOW_ZONE;
				} else {
					state = EventScanningState.MISSED_WINDOW_ZONE;
					prevWindowBinlogInfo = currentWindowBinlogInfo;
					currentWindowBinlogInfo = eventBinlogInfo;
				}
			}

			// The interval corresponding to binlogInfo has been found
			if (state == FOUND_WINDOW_ZONE) {
				return eventIterator;
			}

			// searchBinlogInfo not found, set searchBinlogInfo to the value of the first >searchBinlogInfo found to address
			if (state == EventScanningState.MISSED_WINDOW_ZONE) {
				log.info("Could not serve target binlogInfo: " + binlogInfo + ". Setting serve binlogInfo to: " + prevWindowBinlogInfo);
				return getInternalIterator(prevWindowBinlogInfo, iteratorName);
			}

			throw new PtubesRunTimeException("getInternalIterator fail, binlogInfo: " + binlogInfo);
		}
		throw new PtubesRunTimeException("getInternalIterator fail, binlogInfo: " + binlogInfo);
	}

	public void append(ChangeEntry event) throws Exception {
		if (!event.canAppend(getLastWrittenBinlogInfo(), storageConfig.getIndexPolicy())) {
			log.info("Skipping this transaction, already be written to buffer. Txn binloginfo =" + event.getBinlogInfo()
					+ ", _eventBuffer.lastWrittenBinlogInfo=" + getLastWrittenBinlogInfo());
			return;
		}

		try {
			startEvents();

			int writtenEventLength = appendEvent(event);

			if (writtenEventLength <= 0) {
				throw new PtubesRunTimeException("append event to buffer error, eventInfo: " + event.getBinlogInfo());
			}
		} catch (Exception e) {
			resetWindowState();
			throw e;
		}

	}
}
