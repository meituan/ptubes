package com.meituan.ptubes.reader.storage.channel;

import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.LifeCycle;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import java.io.IOException;
import com.meituan.ptubes.common.exception.GreaterThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;

public interface ReadChannel extends LifeCycle {

	void openOldest() throws PtubesException, IOException;

	void openLatest()  throws PtubesException, IOException;

	void open(BinlogInfo binlogInfo)
			throws PtubesException, IOException, LessThanStorageRangeException, GreaterThanStorageRangeException;

	PtubesEvent next() throws IOException;

	StorageConstant.StorageStatus getStatus();

	StorageConstant.StorageMode getStorageMode();

	String getBinlogInfoRange();

	void switchStorageMode(StorageConstant.StorageMode storageMode)
			throws InterruptedException, LessThanStorageRangeException;

}
