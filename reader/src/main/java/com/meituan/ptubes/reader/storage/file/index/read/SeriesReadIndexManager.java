package com.meituan.ptubes.reader.storage.file.index.read;

import com.meituan.ptubes.common.exception.GreaterThanStorageRangeException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.index.SeriesIndexManagerFinder;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import org.apache.commons.lang3.tuple.Pair;


public final class SeriesReadIndexManager extends AbstractLifeCycle
		implements ReadIndexManager<BinlogInfo, DataPosition> {
	private final StorageConfig storageConfig;
	private final SourceType sourceType;
	private L1ReadIndexManager l1ReadIndexManager;
	private L2ReadIndexManager l2ReadIndexManager;

	public SeriesReadIndexManager(StorageConfig storageConfig, SourceType sourceType) {
		this.storageConfig = storageConfig;
		this.sourceType = sourceType;
	}

	@Override
	protected void doStart() {
		if (l1ReadIndexManager != null) {
			l1ReadIndexManager.start();
		}

		if (l2ReadIndexManager != null) {
			l2ReadIndexManager.start();
		}
	}

	@Override
	protected void doStop() {
		if (l1ReadIndexManager != null) {
			l1ReadIndexManager.stop();
		}

		if (l2ReadIndexManager != null) {
			l2ReadIndexManager.stop();
		}
	}

	@Override
	public Pair<BinlogInfo, DataPosition> findOldest() throws IOException {
		checkStop();

		doStop();

		l1ReadIndexManager = SeriesIndexManagerFinder.findL1ReadIndexManager(storageConfig, sourceType);
		l1ReadIndexManager.start();
		Pair<BinlogInfo, DataPosition> l1IndexEntry = l1ReadIndexManager.findOldest();

		// The expired data in l1Index is not deleted in real time. To avoid errors in finding the oldest point when the expired data is not deleted, the following judgment logic is added.
		DataPosition targetDataPosition = l1IndexEntry.getValue();
		while (!SeriesIndexManagerFinder.isL2IndexFileExist(storageConfig, targetDataPosition)) {
			l1IndexEntry = l1ReadIndexManager.next();
			targetDataPosition = l1IndexEntry.getValue();
		}

		l2ReadIndexManager = SeriesIndexManagerFinder.findL2ReadIndexManager(storageConfig, targetDataPosition, sourceType);
		l2ReadIndexManager.start();
		return l2ReadIndexManager.findOldest();
	}

	@Override
	public Pair<BinlogInfo, DataPosition> findLatest() throws IOException {
		checkStop();

		doStop();

		l1ReadIndexManager = SeriesIndexManagerFinder.findL1ReadIndexManager(storageConfig, sourceType);
		l1ReadIndexManager.start();
		Pair<BinlogInfo, DataPosition> l1IndexEntry = l1ReadIndexManager.findLatest();

		l2ReadIndexManager = SeriesIndexManagerFinder.findL2ReadIndexManager(storageConfig, l1IndexEntry.getValue(), sourceType);
		l2ReadIndexManager.start();
		return l2ReadIndexManager.findLatest();
	}

	@Override
	public Pair<BinlogInfo, DataPosition> find(BinlogInfo indexKey)
			throws IOException, LessThanStorageRangeException, GreaterThanStorageRangeException {
		checkStop();

		doStop();

		l1ReadIndexManager = SeriesIndexManagerFinder.findL1ReadIndexManager(storageConfig, sourceType);
		l1ReadIndexManager.start();

		Pair<BinlogInfo, DataPosition> l1IndexEntry = l1ReadIndexManager.find(indexKey);
		if (l1IndexEntry == null) {
			l1ReadIndexManager.stop();
			l1ReadIndexManager.start();
			l1IndexEntry = l1ReadIndexManager.findLatest();
			if (l1IndexEntry == null) {
				throw new IOException("failed to find binlog info.");
			}
		}

		l2ReadIndexManager = SeriesIndexManagerFinder.findL2ReadIndexManager(storageConfig, l1IndexEntry.getValue(), sourceType);
		l2ReadIndexManager.start();
		return l2ReadIndexManager.find(indexKey);
	}
}
