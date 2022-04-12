package com.meituan.ptubes.storage.mem.mysql;

import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.ErrorEvent;
import com.meituan.ptubes.reader.storage.mem.MemStorage;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import java.util.Objects;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.storage.channel.DefaultReadChannel;
import com.meituan.ptubes.reader.storage.filter.SourceAndPartitionEventFilter;
import com.meituan.ptubes.reader.storage.manager.read.MemReadManager;
import com.meituan.ptubes.storage.WriteReadBaseTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MySQLMemInvalidTest extends WriteReadBaseTest {
	private static final Logger LOG = LoggerFactory.getLogger(MySQLMemWriteReadTest.class);
	private final static String NAME = MySQLMemInvalidTest.class.getName();

	@Parameterized.Parameters
	public static Object[][] data() {
		return new Object[100][0]; // repeat count which you want
	}

	@Before
	public void start() {
		StorageConfig storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY,
				INDEX_POLICY, StorageConstant.StorageMode.MEM, 500 * StorageConstant.KB, NAME);
		start(storageConfig, 10);
	}

	@After
	public void stop() {
		clean();
	}

	@Test
	public void overWriteTest() throws Exception {
		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter,
				readerTaskStatMetricsCollector, SourceType.MySQL);
		readChannel.start();

		try {
			readChannel.open(startBinlogInfo);

			MemStorage memStorage = ((MemReadManager) readChannel.getReadManager()).getMemStorage();
			while (startBinlogInfo.isEqualTo(memStorage.getMinBinlogInfo(), storageConfig.getIndexPolicy())) {
				appendEvent(binlogInfos.get(binlogInfos.size() - 1));
			}

			System.out.println("startBinlogInfo: " + startBinlogInfo.toString());
			System.out.println("memMinBinlogInfo: " + memStorage.getMinBinlogInfo().toString());
			System.out.println("binlogInfos(0): " + binlogInfos.get(0).toString());

			PtubesEvent event = readChannel.next();
			System.out.println("readNextBinlogInfo: " + (Objects.nonNull(event.getBinlogInfo()) ? event.getBinlogInfo().toString() : event.toString()));

			Assert.assertTrue(ErrorEvent.NOT_IN_BUFFER.equals(event));
		} catch (Throwable te) {
			displayTestUnitCases(eventFilter);
			throw te;
		} finally {
			try {
				readChannel.stop();
			} catch (Throwable te) {
				LOG.error("stop write channel error", te);
			}

			try {
				stop();
			} catch (Throwable te) {
				LOG.error("stop write channel error", te);
			}
		}

	}

	@Test
	public void greaterThanMaxBinlogInfoTest() throws Exception {
		greaterThanMaxBinlogInfo();
	}

	@Test
	public void lessThanMaxBinlogInfoTest() throws Exception {
		lessThanMaxBinlogInfo();
	}

}
