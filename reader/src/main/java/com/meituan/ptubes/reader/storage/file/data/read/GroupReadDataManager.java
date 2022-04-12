package com.meituan.ptubes.reader.storage.file.data.read;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.ErrorEvent;
import java.io.EOFException;
import java.io.IOException;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.data.DataManagerFinder;


public class GroupReadDataManager extends AbstractLifeCycle implements ReadDataManager<DataPosition, PtubesEvent> {
	private final static Logger LOG = LoggerFactory.getLogger(GroupReadDataManager.class);
	protected final StorageConfig storageConfig;

	private SingleReadDataManager readDataManager;

	public GroupReadDataManager(StorageConfig storageConfig) {
		this.storageConfig = storageConfig;
	}

	@Override
	protected void doStart() {
		//do nothing
	}

	@Override
	protected void doStop() {
		if (readDataManager != null) {
			readDataManager.stop();
		}
	}

	@Override
	public void open(DataPosition dataKey) throws IOException {
		checkStop();

		readDataManager = DataManagerFinder.findReadDataManager(storageConfig.getReaderTaskName(),
                                                                storageConfig.getFileConfig(), dataKey);
		if (readDataManager == null) {
			throw new IOException("failed to open group read data manager.");
		}
	}

	@Override
	public PtubesEvent next() throws IOException {
		checkStop();

		try {
			return readDataManager.next();
		} catch (EOFException eof) {
			DataPosition dataKey = readDataManager.position();

			SingleReadDataManager tempReadDataManager = DataManagerFinder.findNextReadDataManager(
					storageConfig.getReaderTaskName(), storageConfig.getFileConfig(), dataKey);
			if (tempReadDataManager == null) {
				return ErrorEvent.NO_MORE_EVENT;
			}

			readDataManager.stop();
			tempReadDataManager.start();

			readDataManager = tempReadDataManager;
			
			LOG.info("Read data from new bucket: " + readDataManager.position());
			return readDataManager.next();
		} catch (IOException e) {
			return ErrorEvent.NO_MORE_EVENT;
		}

	}

	@Override
	public DataPosition position() {
		checkStop();

		return readDataManager == null ? null : readDataManager.position();
	}
}
