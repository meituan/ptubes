package com.meituan.ptubes.storage;

import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.channel.DefaultReadChannel;
import com.meituan.ptubes.reader.storage.channel.DefaultWriteChannel;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.reader.storage.filter.PartitionEventFilter;
import com.meituan.ptubes.reader.storage.filter.SourceAndPartitionEventFilter;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import com.meituan.ptubes.storage.utils.FileUtil;
import com.meituan.ptubes.storage.utils.TableUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.storage.manager.write.MemWriteManager;
import com.meituan.ptubes.reader.storage.manager.write.MixWriteManager;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class StorageModeSwitchTest {
	private final static Logger LOG = LoggerFactory.getLogger(StorageModeSwitchTest.class);
	private final static String READER_TASK_NAME = "StorageModeSwitchTest";
	protected static StorageConstant.IndexPolicy INDEX_POLICY = StorageConstant.IndexPolicy.BINLOG_OFFSET;
	private static MySQLBinlogInfo startBinlogInfo = new MySQLBinlogInfo((short) 0, RandomUtils.nextInt(0, Integer.MAX_VALUE), 1, 10L,
			RandomUtils.nextBytes(16), 1L, 0, System.currentTimeMillis());
	private DefaultWriteChannel writeChannel = null;
	private ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;

	private void clearDir() {
		File dir = new File(FileSystem.DEFAULT_PATH + "/" + READER_TASK_NAME);
		if (dir.exists()) {
			FileUtil.delFile(dir);
		}
	}

	public void setUp(StorageConfig storageConfig) throws Exception {
		clearDir();

		readerTaskStatMetricsCollector = new ReaderTaskStatMetricsCollector(storageConfig.getReaderTaskName());

		writeChannel = new DefaultWriteChannel(storageConfig, startBinlogInfo, readerTaskStatMetricsCollector, SourceType.MySQL);
		writeChannel.start();
		try {
			writeChannel.getWriteManager().flush();
		} catch (IOException e) {
		}
	}

	public void tearDown() throws Exception {
		if (writeChannel != null) {
			writeChannel.stop();
		}

		clearDir();
	}

	private SourceAndPartitionEventFilter genEventFilter() {
		HashMap<String, PartitionEventFilter> sources = new HashMap<>();
		sources.put(TableUtil.TEST01_TABLE_NAME, new PartitionEventFilter(true, null, 0));
		SourceAndPartitionEventFilter eventFilter = new SourceAndPartitionEventFilter(sources);
		return eventFilter;
	}

	// Waiting for readTask to read no data for more than noMoreEventTimeMS
	private void waitForReadComplete(ReadTask readTask, long noMoreEventTimeMS) {
		long startTime = System.currentTimeMillis();
		while (System.currentTimeMillis() < startTime + noMoreEventTimeMS) {
			if (!readTask.isNoMoreEvent()) {
				startTime = System.currentTimeMillis();
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}

	// Waiting for readTask to not read data for more than noMoreEventTimeMS
	private void waitForOverWriteMemHead(MemWriteManager memWriteManager) {
		while (true) {
			if (!startBinlogInfo.isEqualTo(memWriteManager.getMemStorage().getMinBinlogInfo(), INDEX_POLICY)) {
				break;
			}
		}
	}

	private void checkAndStopReadTask(ReadTask readTask, int exptEventCount) throws InterruptedException {
		waitForReadComplete(readTask, 1000);
		readTask.shutdown();
		LOG.info("Write eventCount: {}, readDataCount: {}, readCommitCount: {}, readOtherEventCount: {}",
				exptEventCount, readTask.getDataEventCount(), readTask.getCommitEventCount(),
				readTask.getOtherEventCount());
		Assert.assertTrue(exptEventCount > 0);
		Assert.assertEquals(exptEventCount, readTask.getDataEventCount());
		Assert.assertEquals(exptEventCount, readTask.getCommitEventCount());
		Assert.assertEquals(0, readTask.getOtherEventCount());
	}

	@Test
	public void switchNormal() throws Exception {
		StorageConfig storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY,
				INDEX_POLICY, StorageConstant.StorageMode.MEM, 500 * StorageConstant.MB, READER_TASK_NAME);
		setUp(storageConfig);
		Assert.assertEquals(StorageConstant.StorageMode.MEM, writeChannel.getStorageMode());

		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, genEventFilter(), readerTaskStatMetricsCollector, SourceType.MySQL);
		readChannel.start();
		readChannel.openLatest();
		// mem to mix has no data
		this.writeChannel.switchStorageMode(StorageConstant.StorageMode.MIX);
		Assert.assertEquals(StorageConstant.StorageMode.MIX, this.writeChannel.getStorageMode());
		Pair<BinlogInfo, BinlogInfo> storageRange = this.writeChannel.getBinlogInfoRange();
		Assert.assertTrue(this.startBinlogInfo.isEqualTo(storageRange.getLeft(), INDEX_POLICY));
		Assert.assertTrue(this.startBinlogInfo.isEqualTo(storageRange.getRight(), INDEX_POLICY));

		readChannel.switchStorageMode(StorageConstant.StorageMode.MIX);
		Assert.assertEquals(StorageConstant.StorageMode.MIX, readChannel.getStorageMode());

		WriteTask writeTask = new WriteTask(READER_TASK_NAME + "_" + "writeTask", this.writeChannel,
				this.startBinlogInfo);
		ReadTask readTask = new ReadTask(READER_TASK_NAME + "_" + "readTask1", readChannel, this.startBinlogInfo);
		writeTask.start();
		readTask.start();

		// append event
		Thread.sleep(2000);

		readTask.switchStorageMode(StorageConstant.StorageMode.MEM);
		writeTask.switchStorageMode(StorageConstant.StorageMode.MEM);

		Thread.sleep(2000);

		writeTask.switchStorageMode(StorageConstant.StorageMode.MIX);
		readTask.switchStorageMode(StorageConstant.StorageMode.MIX);

		Thread.sleep(2000);

		writeTask.shutdown();
		checkAndStopReadTask(readTask, writeTask.getEventCount());

		tearDown();
	}

	@Test
	public void mixToMemLessThanMinBinlogInfo() throws Exception {
		StorageConfig storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY,
				INDEX_POLICY, StorageConstant.StorageMode.MIX, 5 * StorageConstant.KB, READER_TASK_NAME);
		setUp(storageConfig);

		Assert.assertEquals(StorageConstant.StorageMode.MIX, writeChannel.getStorageMode());

		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, genEventFilter(), readerTaskStatMetricsCollector, SourceType.MySQL);
		readChannel.start();
		readChannel.openLatest();

		WriteTask writeTask = new WriteTask(READER_TASK_NAME + "_" + "writeTask", this.writeChannel,
				this.startBinlogInfo);
		writeTask.start();

		waitForOverWriteMemHead(((MixWriteManager) this.writeChannel.getWriteManager()).getMemWriteManager());

		try {
			readChannel.switchStorageMode(StorageConstant.StorageMode.MEM);
		} catch (LessThanStorageRangeException e) {
			Assert.assertTrue(true);
			return;
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(false);
			return;
		} finally {
			writeTask.shutdown();
			readChannel.stop();
			tearDown();
		}

		Assert.assertTrue(false);
	}

	@Test
	public void withMultiReadChannel() throws Exception {
		StorageConfig storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY,
				INDEX_POLICY, StorageConstant.StorageMode.MEM, 500 * StorageConstant.MB, READER_TASK_NAME);
		setUp(storageConfig);
		Assert.assertEquals(StorageConstant.StorageMode.MEM, writeChannel.getStorageMode());

		List<ReadTask> readTasks = new ArrayList<>();
		List<DefaultReadChannel> readChannels = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, genEventFilter(), readerTaskStatMetricsCollector, SourceType.MySQL);
			readChannel.start();
			readChannel.openLatest();
			readChannels.add(readChannel);
			ReadTask readTask = new ReadTask(READER_TASK_NAME + "_" + "readTask" + i, readChannel,
					this.startBinlogInfo);
			readTask.start();
			readTasks.add(readTask);
		}

		WriteTask writeTask = new WriteTask(READER_TASK_NAME + "_" + "writeTask", this.writeChannel,
				this.startBinlogInfo);
		writeTask.start();

		Thread.sleep(2000);

		writeTask.switchStorageMode(StorageConstant.StorageMode.MIX);
		for (ReadTask readTask : readTasks) {
			readTask.switchStorageMode(StorageConstant.StorageMode.MIX);
		}

		Thread.sleep(2000);

		for (ReadTask readTask : readTasks) {
			readTask.switchStorageMode(StorageConstant.StorageMode.MEM);
		}
		writeTask.switchStorageMode(StorageConstant.StorageMode.MEM);

		Thread.sleep(2000);

		writeTask.switchStorageMode(StorageConstant.StorageMode.MIX);
		for (ReadTask readTask : readTasks) {
			readTask.switchStorageMode(StorageConstant.StorageMode.MIX);
		}

		writeTask.shutdown();
		for (ReadTask readTask : readTasks) {
			checkAndStopReadTask(readTask, writeTask.getEventCount());

		}

		tearDown();
	}
}
