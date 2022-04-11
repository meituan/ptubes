package com.meituan.ptubes.reader.storage.channel;

import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.LifeCycle;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import org.apache.commons.lang3.tuple.Pair;


public interface WriteChannel extends LifeCycle {
	void append(ChangeEntry changeEntry) throws Exception;

	void delete(String date) throws Exception;

	Pair<BinlogInfo, BinlogInfo> getBinlogInfoRange();

	StorageConstant.StorageRangeCheckResult isInStorage(BinlogInfo binlogInfo);

	void switchStorageMode(StorageConstant.StorageMode storageMode) throws InterruptedException;

	void applyFileRetentionConfigChange(int fileRetentionHours, int fileCompressHours) throws InterruptedException;

	StorageConstant.StorageMode getStorageMode();
}
