package com.meituan.ptubes.storage.filter;

import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.ErrorEvent;
import com.meituan.ptubes.storage.mem.mysql.MySQLMemWriteReadTest;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import java.util.HashMap;
import java.util.HashSet;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.storage.channel.DefaultReadChannel;
import com.meituan.ptubes.reader.storage.filter.PartitionEventFilter;
import com.meituan.ptubes.reader.storage.filter.SourceAndPartitionEventFilter;
import com.meituan.ptubes.storage.WriteReadBaseTest;
import com.meituan.ptubes.storage.utils.TableUtil;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReadChannelFilterTest extends WriteReadBaseTest {

	private static final Logger LOG = LoggerFactory.getLogger(MySQLMemWriteReadTest.class);

	@Before
	public void setUp() {
		storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY, INDEX_POLICY,
				StorageConstant.StorageMode.MEM, 500 * StorageConstant.MB, "ReadChannelFilterTest");
		start(storageConfig, 10);

	}

	@After
	public void tearDown() {
		clean();
	}

	@Test
	public void test() throws Exception {
		HashMap<String, PartitionEventFilter> sources = new HashMap<>();
		sources.put(TableUtil.TEST01_TABLE_NAME,
				new PartitionEventFilter(false, new HashSet<>(java.util.Arrays.asList(0, 1, 2, 3, 4)), 5));
		SourceAndPartitionEventFilter eventFilter = new SourceAndPartitionEventFilter(sources);
		DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter,
				readerTaskStatMetricsCollector, SourceType.MySQL);
		readChannel.start();
		int index = RandomUtils.nextInt(0, binlogInfos.size() - 2);
		readChannel.open(binlogInfos.get(index));

		for (int i = index + 1; i < binlogInfos.size(); i++) {
			if (dbChangeEntries.get(i).getTableName().equals(TableUtil.TEST02_TABLE_NAME)) {
				continue;
			}
			PtubesEvent ptubesEvent = readChannel.next();
			checkEqual(dbChangeEntries.get(i), ptubesEvent);
		}

		PtubesEvent ptubesEvent = readChannel.next();
		Assert.assertEquals(ptubesEvent, ErrorEvent.NO_MORE_EVENT);

		readChannel.stop();
	}
}
