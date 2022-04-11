package com.meituan.ptubes.reader.storage.channel;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import java.util.ArrayList;
import java.util.List;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import org.apache.commons.lang3.tuple.Pair;


public class MockedWriteChannel extends AbstractLifeCycle implements WriteChannel {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultWriteChannel.class);
	List<ChangeEntry> changeEntryList = new ArrayList<>();

	public MockedWriteChannel() {
	}

	@Override
	public void append(ChangeEntry dataValue) throws Exception {
		checkStop();
		
		LOG.info("Append DbChangeEntry: " + dataValue);
		changeEntryList.add(dataValue);
	}

	@Override
	public void delete(String date) throws Exception {
	}

	@Override
	protected void doStart() {

	}

	@Override
	protected void doStop() {
	}

	@Override
	public Pair<BinlogInfo, BinlogInfo> getBinlogInfoRange() {
		return null;
	}

	@Override
	public StorageConstant.StorageRangeCheckResult isInStorage(BinlogInfo binlogInfo) {
		return null;
	}

	@Override
	public void switchStorageMode(StorageConstant.StorageMode storageMode) {

	}

	@Override
	public void applyFileRetentionConfigChange(
		int fileRetentionHours,
		int fileCompressHours
	) throws InterruptedException {

	}

	@Override
	public StorageConstant.StorageMode getStorageMode() {
		return null;
	}
}
