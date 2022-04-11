package com.meituan.ptubes.storage.file.mysql;

import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import com.meituan.ptubes.storage.WriteReadBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLFileInvalidTest extends WriteReadBaseTest {
	private final static String NAME = MySQLFileInvalidTest.class.getName();

	@Before
	public void start() {
		StorageConfig storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY, INDEX_POLICY,
				StorageConstant.StorageMode.FILE, 500 * StorageConstant.MB, NAME);
		start(storageConfig, 10, SourceType.MySQL);

	}

	@After
	public void stop() {
		clean();
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
