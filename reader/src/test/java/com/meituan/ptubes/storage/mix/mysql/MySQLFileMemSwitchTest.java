package com.meituan.ptubes.storage.mix.mysql;

import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.mem.MemStorage;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.storage.channel.DefaultReadChannel;
import com.meituan.ptubes.reader.storage.channel.DefaultWriteChannel;
import com.meituan.ptubes.reader.storage.filter.SourceAndPartitionEventFilter;
import com.meituan.ptubes.reader.storage.manager.write.MixWriteManager;
import com.meituan.ptubes.storage.WriteReadBaseTest;
import com.meituan.ptubes.storage.mem.mysql.MySQLMemWriteReadTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MySQLFileMemSwitchTest extends WriteReadBaseTest {
	private static final Logger LOG = LoggerFactory.getLogger(MySQLMemWriteReadTest.class);
	private final static String NAME = MySQLFileMemSwitchTest.class.getName();

	@Parameterized.Parameters
	public static Object[][] data() {
		return new Object[5][0]; // repeat count which you want
	}

	@Before
	public void start() {
		storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY, INDEX_POLICY,
				StorageConstant.StorageMode.MIX, 100 * StorageConstant.KB, NAME);
		start(storageConfig, 10, SourceType.MySQL);

	}

	@After
	public void stop() {
		clean();
	}

	@Test
	public void switchTest() throws Exception {
		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		MySQLBinlogInfo binlogInfo = (MySQLBinlogInfo) binlogInfos.get(0);
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter,
				readerTaskStatMetricsCollector, SourceType.MySQL);
		readChannel.start();

		try {
			readChannel.open(binlogInfo);

			PtubesEvent event = readChannel.next();
			checkEqual(dbChangeEntries.get(1), event);

			Assert.assertTrue(readChannel.getReadManager().getCurrentStorageMode().equals(StorageConstant.StorageMode.MEM));

			MemStorage memStorage = ((MixWriteManager) ((DefaultWriteChannel) writeChannel).getWriteManager())
				.getMemWriteManager().getMemStorage();
			while (!memStorage.getMinBinlogInfo().isGreaterThan(binlogInfos.get(2), storageConfig.getIndexPolicy())) {
				appendEvent(binlogInfos.get(binlogInfos.size() - 1));
			}

			event = readChannel.next();
			checkEqual(dbChangeEntries.get(2), event);
			Assert.assertTrue(
				readChannel.getReadManager().getCurrentStorageMode().equals(StorageConstant.StorageMode.FILE));

			MySQLBinlogInfo memMinBinlogInfo = (MySQLBinlogInfo)memStorage.getMinBinlogInfo();
			for (int i = 3; i < binlogInfos.size(); i++) {
				event = readChannel.next();
				checkEqual(dbChangeEntries.get(i), event);
				if (memMinBinlogInfo.isGreaterThan(binlogInfos.get(i), INDEX_POLICY)) {
					Assert.assertTrue(
						readChannel.getReadManager().getCurrentStorageMode().equals(StorageConstant.StorageMode.FILE));
				} else {
					Assert.assertTrue(
						readChannel.getReadManager().getCurrentStorageMode().equals(StorageConstant.StorageMode.MEM));
				}
			}
			Assert.assertTrue(readChannel.getReadManager().getCurrentStorageMode().equals(StorageConstant.StorageMode.MEM));
		} catch (Throwable te) {
			displayTestUnitCases(eventFilter);
			throw te;
		} finally {
			readChannel.stop();
		}
	}
}
