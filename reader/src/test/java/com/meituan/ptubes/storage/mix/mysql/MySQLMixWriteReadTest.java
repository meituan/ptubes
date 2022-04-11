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

public class MySQLMixWriteReadTest extends WriteReadBaseTest {
	private static final Logger LOG = LoggerFactory.getLogger(MySQLMemWriteReadTest.class);
	private final static String NAME = MySQLMixWriteReadTest.class.getName();
	private final static int MAX_SLEEPS = 9;

	@Before
	public void start() {
		storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY, INDEX_POLICY,
				StorageConstant.StorageMode.MIX, 500 * StorageConstant.MB, NAME);
		int sleepTimes = 0;
		try {
			start(storageConfig, 10);
		} catch (Throwable te) {
			if (sleepTimes < MAX_SLEEPS) {
				sleepTimes++;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
					// do nothing
				}
			}
			throw te;
		}
	}

	@After
	public void stop() {
		clean();
	}

	// Randomly start reading from the middle
	@Test
	public void readFromMiddle() throws Exception {
		for (int i = 0; i < 15; i++) {
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
}
