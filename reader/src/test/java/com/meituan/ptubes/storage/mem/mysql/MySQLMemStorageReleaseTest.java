package com.meituan.ptubes.storage.mem.mysql;

import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import java.util.ArrayList;
import java.util.List;
import com.meituan.ptubes.reader.storage.channel.DefaultReadChannel;
import com.meituan.ptubes.reader.storage.filter.SourceAndPartitionEventFilter;
import com.meituan.ptubes.reader.storage.mem.MemStorageFactory;
import com.meituan.ptubes.storage.WriteReadBaseTest;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MySQLMemStorageReleaseTest extends WriteReadBaseTest {
	private final static String NAME = MySQLMemStorageReleaseTest.class.getName();

	@Before
	public void start() {
		storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY, INDEX_POLICY,
				StorageConstant.StorageMode.MEM, 50 * StorageConstant.MB, NAME);
		start(storageConfig, 10);

	}

	@After
	public void stop() {
		clean();
	}

	@Test
	public void testRelease() throws Exception {
		List<DefaultReadChannel> readChannelList = new ArrayList<>();
		SourceAndPartitionEventFilter eventFilter = genEventFilter();
		int readChannelCount = RandomUtils.nextInt(1, 100);
		for (int i = 0; i < readChannelCount; i++) {
			DefaultReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter, readerTaskStatMetricsCollector, SourceType.MySQL);
			readChannel.start();
			int index = RandomUtils.nextInt(0, binlogInfos.size() - 2);
			readChannel.open(binlogInfos.get(index));
			readChannelList.add(readChannel);
			Assert.assertEquals(MemStorageFactory.getInstance().getRefCount(NAME), i + 2);
		}

		for (int i = 0; i < readChannelCount; i++) {
			readChannelList.get(i).stop();
			Assert.assertEquals(MemStorageFactory.getInstance().getRefCount(NAME), readChannelCount - i);
		}

		Assert.assertEquals(MemStorageFactory.getInstance().getRefCount(NAME), 1);

		clean();

		Assert.assertEquals(MemStorageFactory.getInstance().getRefCount(NAME), 0);
	}
}
