package com.meituan.ptubes.storage.file;

import com.meituan.ptubes.common.utils.DateUtil;
import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntryFactory;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.storage.channel.DefaultWriteChannel;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.common.event.MySQLChangeEntry;
import com.meituan.ptubes.reader.storage.filter.PartitionEventFilter;
import com.meituan.ptubes.reader.storage.filter.SourceAndPartitionEventFilter;
import com.meituan.ptubes.storage.mem.mysql.MySQLMemWriteReadTest;
import com.meituan.ptubes.storage.utils.DbChangeEntryUtil;
import com.meituan.ptubes.storage.utils.FileUtil;
import com.meituan.ptubes.storage.utils.TableUtil;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class RecoverTest {
	private static final Logger LOG = LoggerFactory.getLogger(MySQLMemWriteReadTest.class);
	private static final String NAME = RecoverTest.class.getName();
	private static int indexEntrySize = MySQLBinlogInfo.getSizeInByte() + DataPosition.getSizeInByte() + 4;

	protected List<MySQLChangeEntry> dbChangeEntries = new ArrayList<>();
	protected List<MySQLBinlogInfo> binlogInfos = new ArrayList<>();
	protected List<DataPosition> dataPositions = new ArrayList<>();
	protected StorageConfig storageConfig;
	protected MySQLBinlogInfo startBinlogInfo;
	protected DefaultWriteChannel writeChannel;
	private ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;
	protected StorageConstant.IndexPolicy INDEX_POLICY = StorageConstant.IndexPolicy.BINLOG_OFFSET;
	private byte[] uuid;
	private File maxL2IndexFile;
	private File maxDataFile;
	private RandomAccessFile dataFile;
	private RandomAccessFile indexFile;
	private FileChannel dataFc;
	private FileChannel indexFc;

	@Before
	public void setUp() throws Exception {
		storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY, INDEX_POLICY,
				StorageConstant.StorageMode.FILE, 500 * StorageConstant.MB, NAME);

		FileUtil.delFile(
				new File(FileSystem.DEFAULT_PATH + "/" + storageConfig.getReaderTaskName() + "/binlogIndex"));
		FileUtil.delFile(new File(FileSystem.DEFAULT_PATH + "/" + storageConfig.getReaderTaskName() + "/storage"));

		byte[] uuid = RandomUtils.nextBytes(16);
		long serverId = RandomUtils.nextInt(0, Integer.MAX_VALUE);
		long startTxnId = 0L;
		long startBinlogOffset = 10L;
		startBinlogInfo = new MySQLBinlogInfo((short) 0, serverId, 1, startBinlogOffset, uuid, startTxnId, 0,
				System.currentTimeMillis());
		this.readerTaskStatMetricsCollector = new ReaderTaskStatMetricsCollector(storageConfig.getReaderTaskName());
		writeChannel = new DefaultWriteChannel(storageConfig, startBinlogInfo, readerTaskStatMetricsCollector, SourceType.MySQL);
		writeChannel.start();

		dbChangeEntries = new ArrayList<>();
		binlogInfos = new ArrayList<>();

		try {
			MySQLBinlogInfo binlogInfo = startBinlogInfo;
			File[] dataFiles = FileSystem.visitDataFiles(NAME, DateUtil.getCurrentDateHourString());
			File maxdataFile = FileSystem.visitMaxDataFile(NAME);
			File maxL2IndexFile = FileSystem.visitMaxL2IndexFile(NAME);
			while (maxdataFile == null || maxL2IndexFile == null || dataFiles.length < 15
					|| maxdataFile.length() < storageConfig.getFileConfig().getDataBucketSizeInByte() / 2
					|| maxL2IndexFile.length() < storageConfig.getFileConfig().getL2BucketSizeInByte() / 2) {
				appendEvent(binlogInfo);
				binlogInfo = binlogInfos.get(binlogInfos.size() - 1);
				dataFiles = FileSystem.visitDataFiles(NAME, DateUtil.getCurrentDateHourString());
				maxdataFile = FileSystem.visitMaxDataFile(NAME);
				maxL2IndexFile = FileSystem.visitMaxL2IndexFile(NAME);
			}
		} catch (Exception e) {
			LOG.error("append error", e);
			Assert.assertTrue(false);
		}

		try {
			writeChannel.getWriteManager().flush();
		} catch (Exception e) {
			e.printStackTrace();
		}

		maxL2IndexFile = FileSystem.visitMaxL2IndexFile(storageConfig.getReaderTaskName());
		maxDataFile = FileSystem.visitMaxDataFile(storageConfig.getReaderTaskName());
		dataFile = new RandomAccessFile(maxDataFile.getAbsolutePath(), "rw");
		indexFile = new RandomAccessFile(maxL2IndexFile.getAbsolutePath(), "rw");
		dataFc = dataFile.getChannel();
		indexFc = indexFile.getChannel();
	}

	@After
	public void tearDown() throws Exception {
		dbChangeEntries = new ArrayList<>();
		binlogInfos = new ArrayList<>();
		dataPositions = new ArrayList<>();
		if (writeChannel != null) {
			writeChannel.stop();
		}
	}

	public int appendEvent(MySQLBinlogInfo binlogInfo) throws Exception {
		// append data
		int dataEventCount = RandomUtils.nextInt(1, 10);
		MySQLBinlogInfo dataBinlogInfo = new MySQLBinlogInfo(binlogInfo.getChangeId(), binlogInfo.getServerId(),
				binlogInfo.getBinlogId(), binlogInfo.getBinlogOffset() + 1, binlogInfo.getUuid(),
				binlogInfo.getTxnId() + 1, 0, System.currentTimeMillis());
		binlogInfos.add(dataBinlogInfo);
		MySQLChangeEntry mySQLChangeEntry = (MySQLChangeEntry) DbChangeEntryUtil.genRandomDbChangeEntry(dataBinlogInfo,
				binlogInfo.getTxnId() % 2 == 0 ? TableUtil.TEST01_TABLE_NAME : TableUtil.TEST02_TABLE_NAME);
		dbChangeEntries.add(mySQLChangeEntry);
		writeChannel.append(mySQLChangeEntry);
		dataPositions.add((DataPosition) writeChannel.getWriteManager().position());

		// append commit
		MySQLBinlogInfo commitBinlogInfo = new MySQLBinlogInfo(binlogInfo.getChangeId(), binlogInfo.getServerId(),
				binlogInfo.getBinlogId(), binlogInfo.getBinlogOffset() + 1, binlogInfo.getUuid(),
				binlogInfo.getTxnId() + 1, 1, System.currentTimeMillis());

		MySQLChangeEntry commitEntry = (MySQLChangeEntry) ChangeEntryFactory.createCommitEntry(commitBinlogInfo);
		dbChangeEntries.add(commitEntry);
		binlogInfos.add(commitBinlogInfo);
		writeChannel.append(commitEntry);

		return dataEventCount + 1;
	}

	public SourceAndPartitionEventFilter genEventFilter() {
		HashMap<String, PartitionEventFilter> sources = new HashMap<>();
		sources.put(TableUtil.TEST01_TABLE_NAME, new PartitionEventFilter(true, null, 0));
		sources.put(TableUtil.TEST02_TABLE_NAME, new PartitionEventFilter(true, null, 0));
		SourceAndPartitionEventFilter eventFilter = new SourceAndPartitionEventFilter(sources);
		return eventFilter;
	}

	private void check() {

	}

	// There is an index, but there is no corresponding data
	//	@Test
	public void indexGreaterThanData() throws Exception {
		long originDataFileSize = dataFc.size();
		long originIndexFileSize = indexFc.size();

		// Artificially damaged index file
		long indexCount = originIndexFileSize / indexEntrySize;
		long truncateEntryCount = 0;
		Pair<BinlogInfo, DataPosition> truncateL2IndexEntry ;
		while (true) {
			truncateEntryCount = RandomUtils.nextLong(0, indexCount - 2);
			truncateL2IndexEntry = FileUtil.findIndexEntry(maxL2IndexFile, storageConfig, (indexCount - truncateEntryCount - 1));
			if (truncateL2IndexEntry.getValue().getCreationDate() == DateUtil.getCurrentDateInteger() && truncateL2IndexEntry.getValue().getBucketNumber() == FileSystem.parseDataNumber(maxDataFile)) {
				LOG.info("indexCount: " + indexCount + ", truncateEntryCount: " + truncateEntryCount);
				break;
			}
		}
		LOG.info("truncateL2IndexEntry: " + truncateL2IndexEntry.getValue());
		dataFc.truncate(truncateL2IndexEntry.getValue().getOffset() + 2);
		dataFc.force(true);
		LOG.info("Data file: " + maxDataFile.getAbsolutePath() + ", truncate from " + originDataFileSize + " to " + (truncateL2IndexEntry.getValue().getOffset() - 1));
		LOG.info("Index file: " + maxL2IndexFile.getAbsolutePath() + ", size: " + indexFc.size());

		FileSystem.recover(storageConfig, SourceType.MySQL);

		Assert.assertEquals((indexCount - truncateEntryCount - 1) * indexEntrySize, indexFc.size());
		Assert.assertEquals(truncateL2IndexEntry.getValue().getOffset(), dataFc.size());
	}

	//There is no index, there is corresponding data
	//@Test
	public void indexLessThanData() throws Exception {
		long originDataFileSize = dataFc.size();
		long originIndexFileSize = indexFc.size();

		// Artificially damaged index file
		long indexCount = originIndexFileSize / indexEntrySize;
		long truncateIndexCount = 0;
		Pair<BinlogInfo, DataPosition> truncateL2IndexEntry ;
		while (true) {
			truncateIndexCount = RandomUtils.nextLong(0, indexCount - 2);
			truncateL2IndexEntry = FileUtil.findIndexEntry(maxL2IndexFile, storageConfig, (indexCount - truncateIndexCount - 1));
			if (truncateL2IndexEntry.getValue().getCreationDate() == DateUtil.getCurrentDateInteger() && truncateL2IndexEntry.getValue().getBucketNumber() == FileSystem.parseDataNumber(maxDataFile)) {
				LOG.info("indexCount: " + indexCount + ", truncateEntryCount: " + truncateIndexCount);
				break;
			}
		}
		LOG.info("truncateL2IndexEntry: " + truncateL2IndexEntry.getValue());
		indexFc.truncate((indexCount - truncateIndexCount) * indexEntrySize + 2);
		indexFc.force(true);
		LOG.info("Data file: " + maxDataFile.getAbsolutePath() + ", truncate from " + originDataFileSize + " to " + (truncateL2IndexEntry.getValue().getOffset() - 1));
		LOG.info("Index file: " + maxL2IndexFile.getAbsolutePath() + ", size: " + indexFc.size());

		FileSystem.recover(storageConfig, SourceType.MySQL);

		Assert.assertEquals((indexCount - truncateIndexCount - 1) * indexEntrySize, indexFc.size());
		Assert.assertEquals(truncateL2IndexEntry.getValue().getOffset(), dataFc.size());
	}

	// The only piece of data in the index is corrupted
	//	@Test
	public void deleteMaxL2IndexFile() throws Exception {
		int indexBucketNumber = FileSystem.parseL2IndexNumber(maxL2IndexFile);

		Pair<BinlogInfo, DataPosition> truncateL2IndexEntry = FileUtil.findIndexEntry(maxL2IndexFile, storageConfig, 0);

		LOG.info("truncateL2IndexEntry: " + truncateL2IndexEntry.getValue());

		// The index file is truncate to 2, and the deletion will be triggered when recover
		indexFc.truncate(2);
		indexFc.force(true);

		//The current largest and complete l2 index file, the last entry of this file will be deleted when recovering
		File currMaxCompleteL2IndexFile = FileSystem.visitL2IndexFile(storageConfig.getReaderTaskName(), DateUtil.getCurrentDateHourString(), indexBucketNumber - 1);
		Pair<BinlogInfo, DataPosition> lastCompleteL2IndexEntry = FileUtil.findIndexEntry(currMaxCompleteL2IndexFile, storageConfig, currMaxCompleteL2IndexFile.length() / indexEntrySize - 1);

		FileSystem.recover(storageConfig, SourceType.MySQL);

		File currMaxDataFile = FileSystem.visitMaxDataFile(storageConfig.getReaderTaskName());
		File currMaxL2IndexFile = FileSystem.visitMaxL2IndexFile(storageConfig.getReaderTaskName());
		int currMaxIndexBucketNumber = FileSystem.parseL2IndexNumber(currMaxL2IndexFile);
		Assert.assertEquals(indexBucketNumber - 1, currMaxIndexBucketNumber);
		Pair<BinlogInfo, DataPosition> lastL2IndexEntry = FileUtil.findIndexEntry(currMaxL2IndexFile, storageConfig, currMaxL2IndexFile.length() / indexEntrySize - 1);
		Assert.assertEquals(lastL2IndexEntry.getValue().getBucketNumber(), FileSystem.parseDataNumber(currMaxDataFile));
		Assert.assertEquals(lastL2IndexEntry.getValue().getCreationDate() + "", FileSystem.parseDataDate(currMaxDataFile));
		Assert.assertEquals(lastCompleteL2IndexEntry.getValue().getOffset(), currMaxDataFile.length());
	}

	// The only piece of data in data is corrupted
	@Test
	public void deleteMaxDataFile() throws Exception {
		int indexBucketNumber = FileSystem.parseL2IndexNumber(maxL2IndexFile);
		int dataBucketNumber = FileSystem.parseDataNumber(maxDataFile);

		Pair<BinlogInfo, DataPosition> truncateL2IndexEntry = FileUtil.findIndexEntry(maxL2IndexFile, storageConfig, 0);

		LOG.info("truncateL2IndexEntry: " + truncateL2IndexEntry.getValue());

		// The index file is truncate to 2, and the deletion will be triggered when recover
		dataFc.truncate(2);
		dataFc.force(true);

		//The current largest complete data file, the last block of this file will be deleted when recovering
		File currMaxCompleteDataFile = FileSystem.visitDataFile(storageConfig.getReaderTaskName(), DateUtil.getCurrentDateHourString(), dataBucketNumber - 1);
		Pair<BinlogInfo, DataPosition> lastCompleteL2IndexEntry = FileUtil.findLatestIndexEntry(storageConfig, new DataPosition(FileSystem.parseDate(currMaxCompleteDataFile), FileSystem.parseDataNumber(currMaxCompleteDataFile), currMaxCompleteDataFile.length()));

		FileSystem.recover(storageConfig, SourceType.MySQL);

		File currMaxDataFile = FileSystem.visitMaxDataFile(storageConfig.getReaderTaskName());
		File currMaxL2IndexFile = FileSystem.visitMaxL2IndexFile(storageConfig.getReaderTaskName());
		LOG.info("currMaxDataFile: " + currMaxDataFile.getName() + ", currMaxL2IndexFile: " + currMaxL2IndexFile.getName());
		Pair<BinlogInfo, DataPosition> lastL2IndexEntry = FileUtil.findIndexEntry(currMaxL2IndexFile, storageConfig, currMaxL2IndexFile.length() / indexEntrySize - 1);

		Assert.assertEquals(lastL2IndexEntry.getValue().getBucketNumber(), FileSystem.parseDataNumber(currMaxDataFile));
		Assert.assertEquals(lastL2IndexEntry.getValue().getCreationDate() + "", FileSystem.parseDataDate(currMaxDataFile));
		Assert.assertEquals(lastL2IndexEntry.getValue().getCreationDate() + "", FileSystem.parseL2IndexDate(currMaxL2IndexFile));
		Assert.assertEquals(lastCompleteL2IndexEntry.getValue().getOffset(), currMaxDataFile.length());
	}
}
