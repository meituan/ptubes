package com.meituan.ptubes.storage;

import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.GreaterThanStorageRangeException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.channel.DefaultReadChannel;
import com.meituan.ptubes.reader.storage.channel.DefaultWriteChannel;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntryFactory;
import com.meituan.ptubes.reader.storage.common.event.ErrorEvent;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.reader.storage.filter.PartitionEventFilter;
import com.meituan.ptubes.reader.storage.filter.SourceAndPartitionEventFilter;
import com.meituan.ptubes.storage.utils.DbChangeEntryUtil;
import com.meituan.ptubes.storage.utils.FileUtil;
import com.meituan.ptubes.storage.utils.TableUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;


public abstract class WriteReadBaseTest {
	private static final Logger LOG = LoggerFactory.getLogger(WriteReadBaseTest.class);
	protected List<ChangeEntry> dbChangeEntries = new ArrayList<>();
	protected List<BinlogInfo> binlogInfos = new ArrayList<>();
	protected StorageConfig storageConfig;
	protected SourceType sourceType;
	protected BinlogInfo startBinlogInfo;
	protected DefaultWriteChannel writeChannel;
	protected StorageConstant.IndexPolicy INDEX_POLICY = StorageConstant.IndexPolicy.BINLOG_OFFSET;
	protected ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;

	public void start(StorageConfig config, int txnCount) {
		this.start(config, txnCount, SourceType.MySQL);
	}

	public void start(StorageConfig config, int txnCount, SourceType sourceType) {
		// Delete files left over from the last test
		storageConfig = config;
		this.sourceType = sourceType;
		cleanDir();

		initStartBinlogInfo(sourceType);

		readerTaskStatMetricsCollector = new ReaderTaskStatMetricsCollector(storageConfig.getReaderTaskName());
		writeChannel = new DefaultWriteChannel(storageConfig, startBinlogInfo, readerTaskStatMetricsCollector, sourceType);
		writeChannel.start();
		try {
			writeChannel.getWriteManager().flush();
		} catch (IOException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}
		checkBinlogInfoRange(startBinlogInfo, startBinlogInfo, new ArrayList<>(), writeChannel.getBinlogInfoRange());

		dbChangeEntries = new ArrayList<>();
		binlogInfos = new ArrayList<>();

		try {
			BinlogInfo binlogInfo = startBinlogInfo;
			for (int i = 0; i < txnCount; i++) {
				int count = appendEvent(binlogInfo);
				binlogInfo = binlogInfos.get(binlogInfos.size() - 1);
			}
		} catch (Exception e) {
			LOG.error("append error", e);
			Assert.assertTrue(false);
		}

		try {
			writeChannel.getWriteManager().flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void initStartBinlogInfo(SourceType sourceType) {
		if (SourceType.MySQL.equals(sourceType)) {
			byte[] uuid = RandomUtils.nextBytes(16);
			long serverId = RandomUtils.nextInt(0, Integer.MAX_VALUE);
			long startTxnId = 1L;
			long startBinlogOffset = 10L;
			this.startBinlogInfo = new MySQLBinlogInfo((short) 0, serverId, 1, startBinlogOffset, uuid, startTxnId, 0,
					System.currentTimeMillis());
		} else {
			throw new UnsupportedOperationException("Unsupport sourceType: " + sourceType);
		}
	}

	public void clean() {
		if (writeChannel != null) {
			writeChannel.stop();
		}
		cleanDir();
	}

	public void cleanDir() {
		FileUtil.delFile(new File(FileSystem.DEFAULT_PATH + "/" + storageConfig.getReaderTaskName() + "/storage"));
	}

	public SourceAndPartitionEventFilter genEventFilter() {
		HashMap<String, PartitionEventFilter> sources = new HashMap<>();
		sources.put(TableUtil.TEST01_TABLE_NAME, new PartitionEventFilter(false, new HashSet<>(Arrays.asList(0,1,2,3,4)), 5));
		sources.put(TableUtil.TEST02_TABLE_NAME, new PartitionEventFilter(false, new HashSet<>(Arrays.asList(0,1,2,3,4)), 5));
		SourceAndPartitionEventFilter eventFilter = new SourceAndPartitionEventFilter(sources);
		return eventFilter;
	}

	public int appendEvent(BinlogInfo binlogInfo) throws Exception {
		if (SourceType.MySQL.equals(binlogInfo.getSourceType())) {
			return appendMySQLEvent((MySQLBinlogInfo) binlogInfo);
		} else {
			throw new UnsupportedOperationException("Unsupport sourceType: " + sourceType);
		}
	}

	public int appendMySQLEvent(MySQLBinlogInfo binlogInfo) throws Exception {
		// append data
		int dataEventCount = RandomUtils.nextInt(1, 10);
		for (int i = 0; i < dataEventCount; i++) {
			MySQLBinlogInfo dataBinlogInfo = new MySQLBinlogInfo(binlogInfo.getChangeId(), binlogInfo.getServerId(),
					binlogInfo.getBinlogId(), binlogInfo.getBinlogOffset() + 1, binlogInfo.getUuid(),
					binlogInfo.getTxnId() + 1, i, System.currentTimeMillis());
			binlogInfos.add(dataBinlogInfo);
			ChangeEntry mySQLChangeEntry = DbChangeEntryUtil.genRandomDbChangeEntry(dataBinlogInfo,
					i % 2 == 0 ? TableUtil.TEST01_TABLE_NAME : TableUtil.TEST02_TABLE_NAME);
			dbChangeEntries.add(mySQLChangeEntry);
			writeChannel.append(mySQLChangeEntry);
		}

		// append commit
		MySQLBinlogInfo commitBinlogInfo = new MySQLBinlogInfo(binlogInfo.getChangeId(), binlogInfo.getServerId(),
				binlogInfo.getBinlogId(), binlogInfo.getBinlogOffset() + 1, binlogInfo.getUuid(),
				binlogInfo.getTxnId() + 1, dataEventCount, System.currentTimeMillis());

		ChangeEntry commitEntry = ChangeEntryFactory.createCommitEntry(commitBinlogInfo);
		dbChangeEntries.add(commitEntry);
		binlogInfos.add(commitBinlogInfo);
		writeChannel.append(commitEntry);

		return dataEventCount + 1;
	}

	public Pair<ChangeEntry, ChangeEntry> appendOneEvent(BinlogInfo binlogInfo) throws Exception {
		if (SourceType.MySQL.equals(binlogInfo.getSourceType())) {
			return appendOneMySQLEvent((MySQLBinlogInfo) binlogInfo);
		} else {
			throw new UnsupportedOperationException("Unsupport sourceType: " + sourceType);
		}
	}

	public void readFromMiddleBase() throws Exception {
		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter, readerTaskStatMetricsCollector, sourceType);
		readChannel.start();
		int index = RandomUtils.nextInt(0, binlogInfos.size() - 2);

		try {
			readChannel.open(binlogInfos.get(index));

			for (int i = index + 1; i < binlogInfos.size(); i++) {
				LOG.info("Read from: " + dbChangeEntries.get(i).toString());
				PtubesEvent ptubesEvent = readChannel.next();
				checkEqual(dbChangeEntries.get(i), ptubesEvent);
			}

			// reopen
			index = RandomUtils.nextInt(0, binlogInfos.size() - 2);
			readChannel.open(binlogInfos.get(index));

			for (int i = index + 1; i < binlogInfos.size(); i++) {
				LOG.info("Read from: " + dbChangeEntries.get(i).toString());
				PtubesEvent ptubesEvent = readChannel.next();
				checkEqual(dbChangeEntries.get(i), ptubesEvent);
			}

			PtubesEvent ptubesEvent = readChannel.next();
			Assert.assertEquals(ptubesEvent, ErrorEvent.NO_MORE_EVENT);

			int addCount = appendEvent(binlogInfos.get(binlogInfos.size() - 1));
			writeChannel.getWriteManager().flush();

			int count = 0;

			PtubesEvent event = readChannel.next();
			while (!event.equals(ErrorEvent.NO_MORE_EVENT)) {
				checkEqual(dbChangeEntries.get(dbChangeEntries.size() - addCount + count), event);
				count++;
				event = readChannel.next();
			}
			Assert.assertEquals(addCount, count);
		} finally {
			readChannel.stop();
		}
	}

	private Pair<ChangeEntry, ChangeEntry> appendOneMySQLEvent(MySQLBinlogInfo binlogInfo) throws Exception {
		// append data
		int dataEventCount = 1;
		MySQLBinlogInfo dataBinlogInfo = new MySQLBinlogInfo(binlogInfo.getChangeId(),
				binlogInfo.getServerId(),
				binlogInfo.getBinlogId(),
				binlogInfo.getBinlogOffset() + 1,
				binlogInfo.getUuid(),
				binlogInfo.getTxnId() + 1,
				0,
				System.currentTimeMillis()
		);
		ChangeEntry dataEntry = DbChangeEntryUtil.genRandomDbChangeEntry(
				dataBinlogInfo,
				TableUtil.TEST01_TABLE_NAME
		);
		writeChannel.append(dataEntry);

		// append commit
		MySQLBinlogInfo commitBinlogInfo = new MySQLBinlogInfo(binlogInfo.getChangeId(),
				binlogInfo.getServerId(),
				binlogInfo.getBinlogId(),
				binlogInfo.getBinlogOffset() + 1,
				binlogInfo.getUuid(),
				binlogInfo.getTxnId() + 1,
				dataEventCount,
				System.currentTimeMillis()
		);

		ChangeEntry commitEntry = ChangeEntryFactory.createCommitEntry(commitBinlogInfo);
		writeChannel.append(commitEntry);

		return Pair.of(
				dataEntry,
				commitEntry
		);
	}

	public void checkEqual(ChangeEntry mySQLChangeEntry, PtubesEvent ptubesEvent) {
		try {
			Assert.assertTrue(!EventType.isErrorEvent(ptubesEvent.getEventType()));
			Assert.assertTrue(ptubesEvent.getBinlogInfo().isEqualTo(mySQLChangeEntry.getBinlogInfo(), INDEX_POLICY));
			Assert.assertTrue(ptubesEvent.getTableName().equals(mySQLChangeEntry.getTableName()));
			Assert.assertTrue(ptubesEvent.getSchemaId() == mySQLChangeEntry.getSchemaId());
			byte[] valueBytes = ptubesEvent.getPayload();
			if (valueBytes.length < 1) {
				return;
			}
			byte[] expectBytes = mySQLChangeEntry.getSerializedRecord();
			Assert.assertEquals(expectBytes.length, valueBytes.length);
			for (int i = 0; i < expectBytes.length; i++) {
				Assert.assertEquals(expectBytes[i], valueBytes[i]);
			}
		} catch (AssertionError e) {
			LOG.error("eventType: {}, expect binlogInfo: {}, actual binlogInfo: {}", ptubesEvent.getEventType(),
					mySQLChangeEntry.getBinlogInfo(), ptubesEvent.getBinlogInfo());
			throw e;
		}
	}

	protected void displayTestUnitCases(SourceAndPartitionEventFilter eventFilter) {
		DefaultReadChannel oldRc = new DefaultReadChannel(storageConfig, eventFilter, readerTaskStatMetricsCollector, sourceType);
		LOG.info("Test case fail, try to print all events ======================================================");
		oldRc.start();
		try {
			oldRc.openOldest();
			for (; ; ) {
				PtubesEvent event = oldRc.next();
				if (event instanceof ErrorEvent) {
					break;
				} else {
					LOG.info(event.toString());
				}
			}
		} catch (Exception ie) {
			LOG.info("oldRc error", ie);
		} finally {
			oldRc.stop();
		}
	}


	public void readFromMiddleToEnd() throws Exception {
		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter,readerTaskStatMetricsCollector, sourceType);
		readChannel.start();
		int index = RandomUtils.nextInt(0, binlogInfos.size() - 2);

		try {
			readChannel.open(binlogInfos.get(index));

			LOG.info("Read from: " + dbChangeEntries.get(index + 1));
			for (int i = index + 1; i < binlogInfos.size(); i++) {
				PtubesEvent ptubesEvent = readChannel.next();
				checkEqual(dbChangeEntries.get(i), ptubesEvent);
			}

			PtubesEvent ptubesEvent = readChannel.next();
			Assert.assertEquals(ptubesEvent, ErrorEvent.NO_MORE_EVENT);
		} finally {
			readChannel.stop();
		}
	}

	public void readFromNotExistBinlogInfo() throws Exception {
		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter, readerTaskStatMetricsCollector, sourceType);
		readChannel.start();

		try {
			BinlogInfo binlogInfo;
			if (SourceType.MySQL.equals(sourceType)) {
				MySQLBinlogInfo mySQLStartBinlogInfo = (MySQLBinlogInfo) startBinlogInfo;
				binlogInfo = new MySQLBinlogInfo(mySQLStartBinlogInfo.getChangeId(), mySQLStartBinlogInfo.getServerId(), mySQLStartBinlogInfo.getBinlogId(), mySQLStartBinlogInfo.getBinlogOffset(), mySQLStartBinlogInfo.getUuid(), mySQLStartBinlogInfo.getTxnId(), mySQLStartBinlogInfo.getEventIndex() + 1, mySQLStartBinlogInfo.getTimestamp());
			} else {
				throw new UnsupportedOperationException("Unsupport sourceType: " + sourceType);
			}

			readChannel.open(binlogInfo);

			LOG.info("Read from: " + dbChangeEntries.get(0));
			for (int i = 0; i < binlogInfos.size(); i++) {
				PtubesEvent ptubesEvent = readChannel.next();
				checkEqual(dbChangeEntries.get(i), ptubesEvent);
			}

			PtubesEvent ptubesEvent = readChannel.next();
			Assert.assertEquals(ptubesEvent, ErrorEvent.NO_MORE_EVENT);
		} finally {
			readChannel.stop();
		}
	}

	// Read from the earliest position
	public void readFromOldestBase() throws Exception {
		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter, readerTaskStatMetricsCollector, sourceType);
		readChannel.start();

		try {
			readChannel.openOldest();
			// Attention: open oldest will consume some data repeatedly
			int i = 0;
			PtubesEvent ptubesEvent = null;
			try {
				for (i = 0; i < binlogInfos.size(); i++) {
					ptubesEvent = readChannel.next();
					checkEqual(dbChangeEntries.get(i), ptubesEvent);
				}
			} catch (Throwable te) {
				boolean found = false;
				i--;
				// attention: go back to find the corresponding data
				while (i >= 0) {
					try {
						checkEqual(dbChangeEntries.get(i), ptubesEvent);
						found = true;
						break;
					} catch (Throwable ite) {

					}
				}
				assert (found == true);
				for (i = i + 1; i < binlogInfos.size(); i++) {
					ptubesEvent = readChannel.next();
					checkEqual(dbChangeEntries.get(i), ptubesEvent);
				}
			}

			ptubesEvent = readChannel.next();
			Assert.assertEquals(ptubesEvent, ErrorEvent.NO_MORE_EVENT);

			int addCount = appendEvent(binlogInfos.get(binlogInfos.size() - 1));
			writeChannel.getWriteManager().flush();

			int count = 0;

			PtubesEvent event = readChannel.next();
			while (!event.equals(ErrorEvent.NO_MORE_EVENT)) {
				checkEqual(dbChangeEntries.get(dbChangeEntries.size() - addCount + count), event);
				count++;
				event = readChannel.next();
			}
			Assert.assertEquals(addCount, count);
		} catch (Throwable te) {
			displayTestUnitCases(eventFilter);
			throw te;
		} finally {
			readChannel.stop();
		}
	}

	// start reading from the latest position
	public void readFromLatestBase() throws Exception {
		BinlogInfo searchBinlogInfo = binlogInfos.get(binlogInfos.size() - 1);
		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter, readerTaskStatMetricsCollector, sourceType);
		readChannel.start();
		readChannel.openLatest();

		try {
			PtubesEvent ptubesEvent = readChannel.next();
			Assert.assertEquals(ptubesEvent, ErrorEvent.NO_MORE_EVENT);

			int addCount = appendEvent(searchBinlogInfo);
			writeChannel.getWriteManager().flush();

			int count = 0;

			PtubesEvent event = readChannel.next();
			while (!event.equals(ErrorEvent.NO_MORE_EVENT)) {
				checkEqual(dbChangeEntries.get(dbChangeEntries.size() - addCount + count), event);
				count++;
				event = readChannel.next();
			}
			Assert.assertEquals(addCount, count);
		} finally {
			displayTestUnitCases(eventFilter);
			readChannel.stop();
		}
	}

	// write one read one from the latest position
	public void readConstantlyBase() throws Exception {
		BinlogInfo searchBinlogInfo = binlogInfos.get(binlogInfos.size() - 1);
		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		DefaultReadChannel readChannel = new DefaultReadChannel(
				storageConfig,
				eventFilter,
				readerTaskStatMetricsCollector, sourceType
		);
		readChannel.start();
		readChannel.openLatest();

		PtubesEvent ptubesEvent = readChannel.next();
		Assert.assertEquals(
                ptubesEvent,
				ErrorEvent.NO_MORE_EVENT
		);

		BinlogInfo newBinlogInfo = searchBinlogInfo;
		for (int i = 0; i < 10000; i++) {
			Pair<ChangeEntry, ChangeEntry> data = appendOneEvent(newBinlogInfo);
			writeChannel.getWriteManager()
					.flush();

			PtubesEvent event = readChannel.next();
			checkEqual(
					data.getLeft(),
					event
			);
			event = readChannel.next();
			checkEqual(
					data.getRight(),
					event
			);
			newBinlogInfo = data.getRight()
					.getBinlogInfo();
		}

		readChannel.stop();
	}

	public void readFromLatestToEnd() throws Exception {
		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter, readerTaskStatMetricsCollector, sourceType);
		readChannel.start();

		try {
			readChannel.openLatest();

			PtubesEvent ptubesEvent = readChannel.next();
			Assert.assertEquals(ptubesEvent, ErrorEvent.NO_MORE_EVENT);
		} finally {
			readChannel.stop();
		}
	}

	public void greaterThanMaxBinlogInfo() throws Exception {
		BinlogInfo maxBinlogInfo = binlogInfos.get(binlogInfos.size() - 1);

		BinlogInfo binlogInfo = genGreaterBinlogInfo(maxBinlogInfo);

		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter, readerTaskStatMetricsCollector, sourceType);
		readChannel.start();
		try {
			readChannel.open(binlogInfo);
		} catch (PtubesException e) {
			Assert.assertTrue(false);
			return;
		} catch (IOException e) {
			Assert.assertTrue(false);
			return;
		} catch (LessThanStorageRangeException e) {
			Assert.assertTrue(false);
			return;
		} catch (GreaterThanStorageRangeException e) {
			Assert.assertTrue(true);
			return;
		} finally {
			readChannel.stop();
		}
		Assert.assertTrue(false);
	}

	public void lessThanMaxBinlogInfo() throws Exception {
		BinlogInfo minBinlogInfo = startBinlogInfo;

		BinlogInfo binlogInfo = genLesserBinlogInfo(minBinlogInfo);

		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter, readerTaskStatMetricsCollector, sourceType);
		readChannel.start();
		try {
			readChannel.open(binlogInfo);
		} catch (PtubesException e) {
			Assert.assertTrue(false);
			return;
		} catch (IOException e) {
			Assert.assertTrue(false);
			return;
		} catch (LessThanStorageRangeException e) {
			Assert.assertTrue(true);
			return;
		} catch (GreaterThanStorageRangeException e) {
			Assert.assertTrue(false);
			return;
		} finally {
			readChannel.stop();
		}
		Assert.assertTrue(false);
	}

	public void readWithMultiThreadBase() throws Exception {
		List<Future> futureList = new ArrayList<>();
		ExecutorService threads = Executors.newFixedThreadPool(60);
		try {
			for (int i = 0; i < 30; i ++) {
				Future future = threads.submit(new Runnable() {
					@Override
					public void run() {
						try {
							readFromMiddleToEnd();
						} catch (Exception e) {
							e.printStackTrace();
							Assert.assertTrue(false);
						}
					}
				});
				futureList.add(future);
			}
			for (int i = 0; i < 30; i ++) {
				Future future = threads.submit(new Runnable() {
					@Override
					public void run() {
						try {
							readFromLatestToEnd();
						} catch (Exception e) {
							e.printStackTrace();
							Assert.assertTrue(false);
						}
					}
				});
				futureList.add(future);
			}
			for (Future future : futureList) {
				future.get();
			}
		} finally {
			threads.shutdownNow();
		}

	}

	public void binlogInfoRangeTest() throws Exception {
		checkBinlogInfoRange(startBinlogInfo, binlogInfos.get(binlogInfos.size() -1), binlogInfos, writeChannel.getBinlogInfoRange());

		appendEvent(binlogInfos.get(binlogInfos.size() - 1));
		writeChannel.getWriteManager().flush();

		checkBinlogInfoRange(startBinlogInfo, binlogInfos.get(binlogInfos.size() -1), binlogInfos, writeChannel.getBinlogInfoRange());

		writeChannel.stop();

		if (!StorageConstant.StorageMode.MEM.equals(storageConfig.getStorageMode())) {
			writeChannel = new DefaultWriteChannel(storageConfig, binlogInfos.get(binlogInfos.size() -1), readerTaskStatMetricsCollector, sourceType);
			writeChannel.start();
			checkBinlogInfoRange(startBinlogInfo, binlogInfos.get(binlogInfos.size() -1), binlogInfos, writeChannel.getBinlogInfoRange());
			writeChannel.stop();
		}
	}

	private void checkBinlogInfoRange(BinlogInfo expectMin, BinlogInfo expectMax,
									  List<BinlogInfo> binlogInfoList, Pair<BinlogInfo, BinlogInfo> actualRange) {
		BinlogInfo lessThanBinlogInfo = genLesserBinlogInfo(expectMin);

		Assert.assertTrue(expectMin.isEqualTo(actualRange.getLeft(), storageConfig.getIndexPolicy()));
		Assert.assertTrue(expectMax.isEqualTo(actualRange.getRight(), storageConfig.getIndexPolicy()));
		Assert.assertEquals(StorageConstant.StorageRangeCheckResult.LESS_THAN_MIN, writeChannel.isInStorage(lessThanBinlogInfo));
		Assert.assertEquals(StorageConstant.StorageRangeCheckResult.IN_RANGE, writeChannel.isInStorage(startBinlogInfo));
		for (BinlogInfo binlogInfo : binlogInfoList) {
			Assert.assertEquals(StorageConstant.StorageRangeCheckResult.IN_RANGE, writeChannel.isInStorage(binlogInfo));
		}

		BinlogInfo greaterThanBinlogInfo = genGreaterBinlogInfo(expectMax);

		Assert.assertEquals(StorageConstant.StorageRangeCheckResult.GREATER_THAN_MAX, writeChannel.isInStorage(greaterThanBinlogInfo));
	}

	private BinlogInfo genGreaterBinlogInfo(BinlogInfo binlogInfo) {
		if (SourceType.MySQL.equals(binlogInfo.getSourceType())) {
			MySQLBinlogInfo mySQLBinlogInfo = (MySQLBinlogInfo) binlogInfo;
			return new MySQLBinlogInfo(mySQLBinlogInfo.getChangeId(), mySQLBinlogInfo.getServerId(), mySQLBinlogInfo.getBinlogId(), mySQLBinlogInfo.getBinlogOffset() + 1, mySQLBinlogInfo.getUuid(), mySQLBinlogInfo.getTxnId(), mySQLBinlogInfo.getEventIndex(), mySQLBinlogInfo.getTimestamp());
		} else {
			throw new UnsupportedOperationException("Unsupport sourceType: " + sourceType);
		}
	}

	private BinlogInfo genLesserBinlogInfo(BinlogInfo binlogInfo) {
		if (SourceType.MySQL.equals(binlogInfo.getSourceType())) {
			MySQLBinlogInfo mySQLBinlogInfo = (MySQLBinlogInfo) binlogInfo;
			return new MySQLBinlogInfo(mySQLBinlogInfo.getChangeId(), mySQLBinlogInfo.getServerId(), mySQLBinlogInfo.getBinlogId(), mySQLBinlogInfo.getBinlogOffset() - 1, mySQLBinlogInfo.getUuid(), mySQLBinlogInfo.getTxnId(), mySQLBinlogInfo.getEventIndex(), mySQLBinlogInfo.getTimestamp());
		} else {
			throw new UnsupportedOperationException("Unsupport sourceType: " + sourceType);
		}
	}

}
