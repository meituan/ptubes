package com.meituan.ptubes.reader.storage.file.index.write;

import com.meituan.ptubes.common.utils.DateUtil;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import java.io.IOException;
import java.util.Date;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.index.SeriesIndexManagerFinder;

public class SeriesWriteIndexManager extends AbstractLifeCycle implements WriteIndexManager<BinlogInfo, DataPosition> {
	private static final Logger LOG = LoggerFactory.getLogger(SeriesWriteIndexManager.class);
	private final SourceType sourceType;
	private final StorageConfig storageConfig;
	private L1WriteIndexManager l1WriteIndexManager;
	private L2WriteIndexManager l2WriteIndexManager;
	private int lastDate;
	private DataPosition lastAppendDataPosition = null;

	public SeriesWriteIndexManager(StorageConfig storageConfig, SourceType sourceType) {
		this.storageConfig = storageConfig;
		this.sourceType = sourceType;
	}

	@Override
	public void append(BinlogInfo indexKey, DataPosition indexVal) throws IOException {
		checkStop();

		if (!needAppend(indexVal)) {
			return;
		}

		boolean needFlush = false;
		if (lastAppendDataPosition == null || lastAppendDataPosition.getBucketNumber() != indexVal.getBucketNumber()) {
			needFlush = true;
		}

		lastAppendDataPosition = indexVal;

		if (needPage(
			MySQLBinlogInfo.getSizeInByte() + DataPosition.getSizeInByte(),
			indexKey.getTs()
		)) {
			openPage(
				indexKey,
				indexVal
			);
			LOG.info("Open new L2 bucket, binlogInfo: " + indexKey + ", dataPosition: " + indexVal);
			DataPosition l2DataPosition = l2WriteIndexManager.position();
			l2WriteIndexManager.append(
				indexKey,
				indexVal
			);
			l2WriteIndexManager.flush();
			l1WriteIndexManager.append(
				indexKey,
				l2DataPosition
			);
			l1WriteIndexManager.flush();
			return;
		}

		l2WriteIndexManager.append(indexKey, indexVal);
		if (needFlush) {
			l2WriteIndexManager.flush();
		}
	}

	@Override
	public void flush() throws IOException {
		checkStop();

		if (l1WriteIndexManager != null) {
			l1WriteIndexManager.flush();
		}

		if (l2WriteIndexManager != null) {
			l2WriteIndexManager.flush();
		}
	}

	@Override
	public void clean(Date date) throws IOException {
		
	}

	@Override
	public DataPosition position() {
		return null;
	}

	@Override
	protected void doStart() {
		// do nothing
	}

	@Override
	protected void doStop() {
		if (l1WriteIndexManager != null) {
			l1WriteIndexManager.stop();
		}

		if (l2WriteIndexManager != null) {
			l2WriteIndexManager.stop();
		}
	}

	protected boolean needPage(
		int needBytes,
		long timestamp
	) {
		// A new bucket will be created in the three scenarios of service restart, file full, and time change.
		return l2WriteIndexManager == null || !l2WriteIndexManager.hasRemainingForWrite(needBytes)
			|| lastDate != DateUtil.getDateHour(timestamp);
	}

	protected void openPage(BinlogInfo indexKey, DataPosition indexVal) throws IOException {
		if (l1WriteIndexManager == null) {
			l1WriteIndexManager = SeriesIndexManagerFinder.findL1WriteIndexManager(storageConfig, sourceType);
			l1WriteIndexManager.start();
		}

		// Flush l2 index before paging.
		if (l2WriteIndexManager != null) {
			l2WriteIndexManager.flush();
			l2WriteIndexManager.stop();
		}

		l2WriteIndexManager = SeriesIndexManagerFinder.findNextL2WriteIndexManager(
			storageConfig,
			DateUtil.getDateHour(indexKey.getTs())
		);
		l2WriteIndexManager.start();

		lastDate = indexVal.getCreationDate();
	}

	public boolean needAppend(DataPosition indexVal) {
		if (lastAppendDataPosition == null) {
			return true;
		}
		if (lastAppendDataPosition.getCreationDate() != indexVal.getCreationDate()
				|| lastAppendDataPosition.getBucketNumber() != indexVal.getBucketNumber()
				|| indexVal.getOffset() - lastAppendDataPosition.getOffset() >= storageConfig.getFileConfig()
				.getBlockSizeInByte()) {
			return true;
		}
		return false;
	}
}
