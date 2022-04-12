package com.meituan.ptubes.reader.storage.file.data.write;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.DateUtil;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.storage.file.data.DataManagerFinder;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.storage.common.DataPosition;

public class GroupWriteDataManager extends AbstractLifeCycle implements WriteDataManager<DataPosition, ChangeEntry> {
	private final static Logger LOG = LoggerFactory.getLogger(GroupWriteDataManager.class);
	protected StorageConfig storageConfig;
	private final BinlogInfo startBinlogInfo;

	private SingleWriteDataManager writeDataManager;

	public GroupWriteDataManager(
		StorageConfig storageConfig,
		BinlogInfo startBinlogInfo
	) {
		this.storageConfig = storageConfig;
		this.startBinlogInfo = startBinlogInfo;
	}

	@Override
	protected void doStart() {
		// do nothing, start writeDataManager when append
		
		try {
			openPage(DateUtil.getDateHour(this.startBinlogInfo.getTs()));
		} catch (IOException e) {
			throw new PtubesRunTimeException("Start group write data manager error: " + e);
		}
	}
	
	@Override
	protected void doStop() {
		if (writeDataManager != null) {
			writeDataManager.stop();
		}
	}

	@Override
	public DataPosition append(ChangeEntry dataValue) throws IOException {
		checkStop();

		byte[] data = dataValue.getSerializedChangeEntry();

		if (needNewPage(data.length, dataValue.getBinlogInfo().getTs())) {
			LOG.info("Open new data bucket, binlogInfo: " + dataValue.getBinlogInfo());
			openPage(DateUtil.getDateHour(dataValue.getBinlogInfo().getTs()));
		}

		DataPosition dataPosition = writeDataManager.append(data);

		return dataPosition;
	}

	@Override
	public void flush() throws IOException {
		checkStop();

		if (writeDataManager != null) {
			writeDataManager.flush();
		}
	}

	@Override
	public DataPosition position() {
		checkStop();

		return writeDataManager.position();
	}

	/**
	 * Do you need to create a new bucket
	 * 1. The current bucket cannot store new data
	 * 2. The new data time range exceeds the current file storage range (in hours)
	 * @param needBytes
	 * @param timestamp
	 * @return
	 */
	protected boolean needNewPage(int needBytes, long timestamp) {
		return writeDataManager == null || !writeDataManager.hasRemainingForWrite(needBytes)
				|| writeDataManager.position().getCreationDate() != DateUtil.getDateHour(timestamp);
	}

	protected void openPage(int dateHour) throws IOException {
		if (writeDataManager != null) {
			writeDataManager.flush();
			writeDataManager.stop();
		}

		writeDataManager = DataManagerFinder.findNextWriteDataManager(storageConfig, dateHour);
		writeDataManager.start();
	}
}
