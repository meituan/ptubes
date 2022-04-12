package com.meituan.ptubes.reader.storage.file.index.write;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.storage.utils.CodecUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.reader.storage.file.index.IndexManagerFactory;
import com.meituan.ptubes.reader.storage.file.index.read.L1ReadIndexManager;
import org.apache.commons.lang3.tuple.Pair;


public class L1WriteIndexManager extends SingleWriteIndexManager<BinlogInfo, DataPosition> {
	private final static Logger LOG = LoggerFactory.getLogger(L1WriteIndexManager.class);
	private final SourceType sourceType;
	private final StorageConfig storageConfig;
	private List<Pair<BinlogInfo, DataPosition>> indexEntryList = new ArrayList<Pair<BinlogInfo, DataPosition>>();

	public L1WriteIndexManager(File file, int bufSizeByte, int maxSizeByte, StorageConfig storageConfig, SourceType sourceType) {
		super(file, bufSizeByte, maxSizeByte);
		this.storageConfig = storageConfig;
		this.sourceType = sourceType;
	}

	public L1WriteIndexManager(File file, String date, int number, int bufSizeByte, int maxSizeByte,
			StorageConfig storageConfig, SourceType sourceType) {
		super(file, date, number, bufSizeByte, maxSizeByte);
		this.storageConfig = storageConfig;
		this.sourceType = sourceType;
	}

	@Override
	public void clean(Date date) throws IOException {
		
	}

	@Override
	public void append(BinlogInfo indexKey, DataPosition indexValue) throws IOException {
		if (!indexEntryList.isEmpty() && isL2IndexFileExist(indexEntryList.get(0).getValue())) {
			indexEntryList.add(Pair.of(indexKey, indexValue));
			super.append(indexKey, indexValue);
		} else {
			loadL1Index();
			indexEntryList.add(Pair.of(indexKey, indexValue));

			FileSystem.backupL1IndexFile(storageConfig.getReaderTaskName());
			FileSystem.nextL1IndexFile(storageConfig.getReaderTaskName());
			doStop();
			super.doStart();

			boolean needAppend = false;
			for (Pair<BinlogInfo, DataPosition> pair : indexEntryList) {
				if (needAppend) {
					super.append(pair.getLeft(), pair.getRight());
				} else {
					if (isL2IndexFileExist(pair.getValue())) {
						needAppend = true;
						super.append(pair.getLeft(), pair.getRight());
					} else {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Skip l1 indexKey: " + indexKey + ", indexValue: " + indexValue);
						}
					}
				}
			}
		}
	}

	@Override
	protected void doStart() {
		super.doStart();
		loadL1Index();
	}

	/**
	 * 	Flush l1 cache from local file
	 */
	private void loadL1Index() {
		indexEntryList.clear();
		L1ReadIndexManager l1IndexReader = IndexManagerFactory.newL1ReadIndexManager(file, storageConfig, sourceType);
		l1IndexReader.start();
		while (true) {
			Pair<BinlogInfo, DataPosition> l1index = null;
			try {
				l1index = l1IndexReader.next();
			} catch (IOException e) {
				LOG.warn("Read next l1Index error", e.getMessage());
				break;
			}

			if (l1index == null) {
				break;
			}

			indexEntryList.add(l1index);
		}
		l1IndexReader.stop();
	}

	private boolean isL2IndexFileExist(DataPosition dataPosition) {
		return FileSystem.l2IndexExist(storageConfig.getReaderTaskName(), dataPosition.getCreationDate(),
				dataPosition.getBucketNumber());
	}

	@Override
	protected byte[] encode(BinlogInfo binlogInfo, DataPosition dataPosition) {
		return CodecUtil.encodeL1Index(binlogInfo, dataPosition);
	}
}
