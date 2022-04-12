package com.meituan.ptubes.storage.mem.mysql;

import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.storage.WriteReadBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLMemWriteReadTest extends WriteReadBaseTest {
	private static final Logger LOG = LoggerFactory.getLogger(MySQLMemWriteReadTest.class);
	private final static String NAME = MySQLMemWriteReadTest.class.getName();

	@Before
	public void start() {
		storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY, INDEX_POLICY,
				StorageConstant.StorageMode.MEM, 5 * StorageConstant.MB, NAME);
		start(storageConfig, 30);

	}

	@After
	public void stop() {
		clean();
	}

	// Randomly start reading from the middle
	@Test
	public void readFromMiddle() throws Exception {
		for (int i = 0; i < 1; i ++) {
			readFromMiddleBase();
		}
	}

	// start reading from the earliest position
	@Test
	public void readFromOldest() throws Exception {
		readFromOldestBase();
	}

	// start reading from the latest position
	@Test
	public void readFromLatest() throws Exception {
		readFromLatestBase();
	}

	// multithreaded read
	@Test
	public void readWithMultiThread() throws Exception {
		readWithMultiThreadBase();
	}

	// Start reading from a non-existing binlogInfo location
	@Test
	public void readFromNotExistBinlogInfo() throws Exception {
		super.readFromNotExistBinlogInfo();
	}

	@Test
	public void binlogInfoRangeTest() throws Exception {
		super.binlogInfoRangeTest();
	}

	@Test
	public void readConstantlyTest() throws Exception {
		super.readConstantlyBase();
	}
}
