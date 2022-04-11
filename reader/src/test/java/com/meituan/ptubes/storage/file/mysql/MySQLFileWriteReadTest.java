package com.meituan.ptubes.storage.file.mysql;

import com.meituan.ptubes.common.utils.GZipUtil;
import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.storage.mem.mysql.MySQLMemWriteReadTest;
import com.meituan.ptubes.storage.utils.ConfigUtil;
import java.io.File;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.storage.WriteReadBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLFileWriteReadTest extends WriteReadBaseTest {
	private static final Logger LOG = LoggerFactory.getLogger(MySQLMemWriteReadTest.class);
	private final static String NAME = MySQLFileWriteReadTest.class.getName();

	@Before
	public void start() {
		storageConfig = ConfigUtil.genStorageConfig(EventBufferConstants.AllocationPolicy.DIRECT_MEMORY, INDEX_POLICY,
				StorageConstant.StorageMode.FILE, 500 * StorageConstant.MB, NAME);
		start(storageConfig, 500);

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

	// Compressed file, random read
	@Test
	public void readWithCompress() throws Exception {
		for (int i = 0; i < 15; i++) {
			compressDataFile();
			readFromMiddleToEnd();
		}
		{
			compressDataFile();
			readFromOldest();
		}
		decompressDataFile();
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

	private void compressDataFile() {
		File[] dateDirs = FileSystem.visitDataDateDirs(NAME);
		for (int i = 0; i < dateDirs.length; i++) {
			File dateDir = dateDirs[i];
			String dateHour = dateDir.getName();
			File[] dataFiles = FileSystem.visitDataFiles(
				NAME,
				dateHour
			);
			for (int j = 0; j < dataFiles.length; j++) {
				File dataFile = dataFiles[j];
				int maxFileNumber = FileSystem.maxFileNumber(
					NAME,
					dateDir.getName()
				);
				try {
					if (!dataFile.getName()
						.endsWith(GZipUtil.EXT)) {
						int fileNumber = FileSystem.parseNumber(
							dataFile,
							FileSystem.DATA_PREFIX,
							FileSystem.DATA_SUFFIX
						);
						if (i == dateDirs.length - 1 && maxFileNumber == fileNumber) {
							break;
						} else {
							GZipUtil.compress(
								dataFile,
								true
							);
							LOG.info(
								"Compress file: {} success",
								dataFile.getAbsolutePath()
							);
						}
					}
				} catch (Exception e) {
					LOG.error(
						"Compress file: {}, error",
						dataFile.getAbsolutePath(),
						e
					);
				}
			}
		}
	}

	private void decompressDataFile() {
		File[] dateDirs = FileSystem.visitDataDateDirs(NAME);
		for (File dateDir : dateDirs) {
			String dateHour = dateDir.getName();
			File[] dataFiles = FileSystem.visitDataFiles(
				NAME,
				dateHour
			);
			for (File dataFile : dataFiles) {
				try {
					if (dataFile.getName()
						.endsWith(GZipUtil.EXT)) {
						GZipUtil.decompress(
							dataFile,
							true
						);
						LOG.info(
							"Decompress file: {} success",
							dataFile.getAbsolutePath()
						);
					}
				} catch (Exception e) {
					LOG.error(
						"Decompress file: {}, error",
						dataFile.getAbsolutePath(),
						e
					);
				}
			}
		}
	}
}
