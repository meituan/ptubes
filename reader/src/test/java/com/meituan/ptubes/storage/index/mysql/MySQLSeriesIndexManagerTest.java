package com.meituan.ptubes.storage.index.mysql;

import com.meituan.ptubes.common.utils.DateUtil;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.reader.storage.file.index.write.SeriesWriteIndexManager;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.index.read.SeriesReadIndexManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MySQLSeriesIndexManagerTest {
	private static final Logger LOG = LoggerFactory.getLogger(MySQLSeriesIndexManagerTest.class);
	private static final String NAME = "indexTest";
	private static final String BASE_DIR = FileSystem.DEFAULT_PATH + "/" + NAME;
	private static final String BASE_DATA_DIR = BASE_DIR + "/storage/" + DateUtil.getCurrentDateInteger();
	private SeriesWriteIndexManager writeIndexManager;
	private StorageConstant.IndexPolicy INDEX_POLICY = StorageConstant.IndexPolicy.BINLOG_OFFSET;
	private StorageConfig storageConfig = ConfigUtil.genStorageConfig(
			EventBufferConstants.AllocationPolicy.DIRECT_MEMORY, INDEX_POLICY, StorageConstant.StorageMode.FILE,
			500 * StorageConstant.MB, NAME);
	private byte[] uuid = RandomUtils.nextBytes(16);
	private long serverId = RandomUtils.nextInt(0, Integer.MAX_VALUE);
	private long startTxnId = 10L;
	private long startBinlogOffset = 10L;

	private List<MySQLBinlogInfo> binlogInfos = new ArrayList<>();
	private List<DataPosition> dataPositions = new ArrayList<>();
	private List<Integer> writeBinlogInfoIds = new ArrayList<>();

	@Before
	public void setUp() throws Exception {
		try {
			FileUtils.forceDelete(new File(BASE_DIR));
		} catch (Exception ignore) {
		}

		binlogInfos.clear();
		dataPositions.clear();
		writeBinlogInfoIds.clear();

		writeIndexManager = new SeriesWriteIndexManager(storageConfig, SourceType.MySQL);
		writeIndexManager.start();
	}

	@After
	public void tearDown() throws Exception {
		try {
			FileUtils.forceDelete(new File(BASE_DIR));
		} catch (Exception ignore) {
		}
		if (writeIndexManager != null) {
			writeIndexManager.stop();
		}
	}

	@Test
	public void normalTest() throws Exception {
		int eventSize = RandomUtils.nextInt(1, storageConfig.getFileConfig().getBlockSizeInByte() - 1);
		File[] l2Indexs = FileSystem.visitL2indexFiles(NAME, DateUtil.getCurrentDateInteger() + "");
		int indexEntryCount = 0;
		long totalEventSize = 0L;
		while (l2Indexs.length <= 6) {
			int bucketNumber = (int) (totalEventSize / storageConfig.getFileConfig().getDataBucketSizeInByte());
			long offset = totalEventSize % storageConfig.getFileConfig().getDataBucketSizeInByte();
			MySQLBinlogInfo binlogInfo = new MySQLBinlogInfo((short) 0, serverId, 1, startBinlogOffset + indexEntryCount, uuid,
					startTxnId + indexEntryCount, 0, System.currentTimeMillis());
			DataPosition dataPosition = new DataPosition(DateUtil.getCurrentDateInteger(), bucketNumber, offset);
			binlogInfos.add(binlogInfo);
			dataPositions.add(dataPosition);
			if (writeIndexManager.needAppend(dataPosition)) {
				writeBinlogInfoIds.add(indexEntryCount);
			}
			writeIndexManager.append(binlogInfo, dataPosition);
			indexEntryCount++;
			totalEventSize += eventSize;
			l2Indexs = FileSystem.visitL2indexFiles(NAME, DateUtil.getCurrentDateInteger() + "");
		}

		writeIndexManager.flush();

		Assert.assertTrue(writeBinlogInfoIds.size() > 1 && writeBinlogInfoIds.size() < binlogInfos.size());

		SeriesReadIndexManager readIndexManager = new SeriesReadIndexManager(storageConfig, SourceType.MySQL);
		readIndexManager.start();

		Pair<BinlogInfo, DataPosition> oldestIndexEntry = readIndexManager.findOldest();
		Assert.assertEquals(dataPositions.get(0), oldestIndexEntry.getValue());

		Pair<BinlogInfo, DataPosition> latestIndexEntry = readIndexManager.findLatest();
		Assert.assertEquals(dataPositions.get(writeBinlogInfoIds.get(writeBinlogInfoIds.size() - 1)),
				latestIndexEntry.getValue());

		for (int i = 0; i < 15; i++) {
			int index = RandomUtils.nextInt(0, binlogInfos.size() - 1);
			MySQLBinlogInfo binlogInfo = binlogInfos.get(index);
			DataPosition dataPosition = dataPositions.get(index);
			Pair<BinlogInfo, DataPosition> actualIndexEntry = readIndexManager.find(binlogInfo);
			DataPosition expectDataPosition = dataPositions.get(writeBinlogInfoIds.get(0));
			for (int dataIndex : writeBinlogInfoIds) {
				if (binlogInfos.get(dataIndex).isGreaterThan(binlogInfo, INDEX_POLICY)) {
					break;
				}
				expectDataPosition = dataPositions.get(dataIndex);
			}
			try {
				Assert.assertEquals(expectDataPosition, actualIndexEntry.getValue());
			} catch (AssertionError e) {
				LOG.error("dataPosition: {}, i: {}", dataPosition, i);
				throw e;
			}
		}
	}

	@Test
	public void needAppendTest() throws Exception {
		SeriesReadIndexManager readIndexManager = new SeriesReadIndexManager(storageConfig, SourceType.MySQL);
		readIndexManager.start();

		{
			MySQLBinlogInfo binlogInfo = new MySQLBinlogInfo((short) 0, serverId, 1, startBinlogOffset + 1, uuid, startTxnId + 1,
					0, System.currentTimeMillis());
			DataPosition dataPosition = new DataPosition(DateUtil.getCurrentDateInteger(), 0, 0);
			binlogInfos.add(binlogInfo);
			dataPositions.add(dataPosition);

			Assert.assertTrue(writeIndexManager.needAppend(dataPosition));
			writeIndexManager.append(binlogInfo, dataPosition);
		}

		{
			MySQLBinlogInfo binlogInfo = new MySQLBinlogInfo((short) 0, serverId, 1, startBinlogOffset + 2, uuid, startTxnId + 2,
					0, System.currentTimeMillis());
			DataPosition dataPosition = new DataPosition(DateUtil.getCurrentDateInteger(), 0,
                                                         storageConfig.getFileConfig().getBlockSizeInByte() + 1);
			binlogInfos.add(binlogInfo);
			dataPositions.add(dataPosition);

			Assert.assertTrue(writeIndexManager.needAppend(dataPosition));
			writeIndexManager.append(binlogInfo, dataPosition);
		}

		{
			MySQLBinlogInfo binlogInfo = new MySQLBinlogInfo((short) 0, serverId, 1, startBinlogOffset + 3, uuid, startTxnId + 3,
					0, System.currentTimeMillis());
			DataPosition dataPosition = new DataPosition(DateUtil.getCurrentDateInteger(), 1,
                                                         storageConfig.getFileConfig().getBlockSizeInByte() + 1);
			binlogInfos.add(binlogInfo);
			dataPositions.add(dataPosition);

			Assert.assertTrue(writeIndexManager.needAppend(dataPosition));
			writeIndexManager.append(binlogInfo, dataPosition);
		}

		writeIndexManager.flush();

		{
			Pair<BinlogInfo, DataPosition> indexEntry = readIndexManager.findOldest();
			Assert.assertEquals(dataPositions.get(0), indexEntry.getValue());
		}

		{
			Pair<BinlogInfo, DataPosition> indexEntry = readIndexManager.find(binlogInfos.get(1));
			Assert.assertEquals(dataPositions.get(1), indexEntry.getValue());
		}

		{
			Pair<BinlogInfo, DataPosition> indexEntry = readIndexManager.findLatest();
			Assert.assertEquals(dataPositions.get(2), indexEntry.getValue());
		}

	}
}
