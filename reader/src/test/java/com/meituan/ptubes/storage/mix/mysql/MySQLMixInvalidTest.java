package com.meituan.ptubes.storage.mix.mysql;

import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.storage.WriteReadBaseTest;
import com.meituan.ptubes.storage.mem.mysql.MySQLMemWriteReadTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLMixInvalidTest extends WriteReadBaseTest {
	private static final Logger LOG = LoggerFactory.getLogger(MySQLMemWriteReadTest.class);
	private final static String NAME = MySQLMixInvalidTest.class.getName();

	@Before
	public void start() {
		storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY, INDEX_POLICY,
				StorageConstant.StorageMode.MIX, 500 * StorageConstant.MB, NAME);
		start(storageConfig, 10);

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
