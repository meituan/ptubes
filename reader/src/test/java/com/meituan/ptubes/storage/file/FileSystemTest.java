package com.meituan.ptubes.storage.file;

import com.meituan.ptubes.common.utils.DateUtil;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class FileSystemTest {
	private final static String NAME = FileSystemTest.class.getName();
	private static final String BASE_DIR = FileSystem.DEFAULT_PATH + "/" + NAME;
	private static final String BASE_DATA_DIR =
			BASE_DIR + File.separator + FileSystem.dataDir + File.separator + DateUtil.getCurrentDateHourString();
	private static final String BASE_L1INDEX_DIR = BASE_DIR + File.separator + FileSystem.l1IndexDir;
	private static final String BASE_L2INDEX_DIR =
			BASE_DIR + File.separator + FileSystem.l2IndexDir + File.separator + DateUtil.getCurrentDateHourString();

	@Before
	public void setUp() throws Exception {
		try {
			FileUtils.forceDelete(new File(BASE_DIR));
		} catch (Exception ignore) {

		}
	}

	@After
	public void tearDown() throws Exception {
		try {
			FileUtils.forceDelete(new File(BASE_DIR));
		} catch (Exception ignore) {

		}
	}

	@Test
	public void parseNumberTest() throws Exception {
		Assert.assertEquals(0, FileSystem.parseNumber(new File(BASE_DATA_DIR, "/bucket-0.data"), "bucket-", ".data"));
		Assert.assertEquals(1, FileSystem.parseNumber(new File(BASE_DATA_DIR, "/bucket-1.l2idx"), "bucket-", ".l2idx"));
		Assert.assertEquals(12, FileSystem.parseNumber(new File(BASE_DATA_DIR, "/bucket-12.data"), "bucket-", ".data"));
	}

	@Test
	public void parseDateTest() throws Exception {
		Assert.assertEquals(DateUtil.getCurrentDateHourString(), FileSystem.parseDate(new File(BASE_DATA_DIR, "/bucket-0.data")));
	}

	@Test
	public void maxFileNumberTest() throws Exception {
		File file0 = new File(BASE_DATA_DIR, "/bucket-0.data");
		createFile(file0);
		File file1 = new File(BASE_DATA_DIR, "/bucket-1.data");
		createFile(file1);
		File file2 = new File(BASE_DATA_DIR, "/bucket-10.data");
		createFile(file2);
		File file3 = new File(BASE_DATA_DIR, "/bucket-7.data");
		createFile(file3);
		Assert.assertEquals(10, FileSystem
				.maxFileNumber("storage/data", NAME, DateUtil.getCurrentDateHourString(), "bucket-", ".data"));

		File file4 = new File(BASE_DATA_DIR, "/bucket-0.data.gz");
		createFile(file4);
		File file5 = new File(BASE_DATA_DIR, "/bucket-1.data.gz");
		createFile(file5);
		Assert.assertEquals(10, FileSystem
			.maxFileNumber("storage/data", NAME, DateUtil.getCurrentDateHourString(), "bucket-", ".data"));

		File file6 = new File(BASE_DATA_DIR, "/bucket-2.data.gz");
		createFile(file6);
		File file7 = new File(BASE_DATA_DIR, "/bucket-11.data.gz");
		createFile(file7);
		Assert.assertEquals(11, FileSystem
			.maxFileNumber("storage/data", NAME, DateUtil.getCurrentDateHourString(), "bucket-", ".data"));
	}

	@Test
	public void nextL1IndexFileTest() throws Exception {
		File file = new File(BASE_L1INDEX_DIR, "/l1Index.l1idx");
		Assert.assertFalse(file.exists());
		Assert.assertEquals(file, FileSystem.nextL1IndexFile(NAME));
		Assert.assertTrue(file.exists());
	}

	@Test
	public void visitL1IndexFileTest() throws Exception {
		File file = new File(BASE_L1INDEX_DIR, "/l1Index.l1idx");
		Assert.assertFalse(file.exists());
		Assert.assertEquals(file, FileSystem.nextL1IndexFile(NAME));
		Assert.assertTrue(file.exists());
		Assert.assertEquals(file, FileSystem.visitL1IndexFile(NAME));
	}

	@Test
	public void nextL2IndexFileTest() throws Exception {
		File file = new File(BASE_L2INDEX_DIR, "/bucket-0.l2idx");
		Assert.assertFalse(file.exists());
		Assert.assertEquals(file, FileSystem.nextL2IndexFile(NAME, DateUtil.getCurrentDateHourString()));
		Assert.assertTrue(file.exists());
		Assert.assertEquals(file, FileSystem.visitL2IndexFile(NAME, DateUtil.getCurrentDateHourString(), 0));
		File file1 = new File(BASE_L2INDEX_DIR, "/bucket-1.l2idx");
		Assert.assertFalse(file1.exists());
		Assert.assertEquals(file1, FileSystem.nextL2IndexFile(NAME, DateUtil.getCurrentDateHourString()));
		Assert.assertTrue(file1.exists());
		Assert.assertEquals(file1, FileSystem.visitL2IndexFile(NAME, DateUtil.getCurrentDateHourString(), 1));
	}

	@Test
	public void nextDataFileTest() throws Exception {
		File file = new File(BASE_DATA_DIR, "/bucket-0.data");
		Assert.assertFalse(file.exists());
		Assert.assertEquals(file, FileSystem.nextDataFile(NAME, DateUtil.getCurrentDateHourString()));
		Assert.assertTrue(file.exists());
		Assert.assertEquals(file, FileSystem.visitDataFile(NAME, DateUtil.getCurrentDateHourString(), 0));
		File file1 = new File(BASE_DATA_DIR, "/bucket-1.data");
		Assert.assertFalse(file1.exists());
		Assert.assertEquals(file1, FileSystem.nextDataFile(NAME, DateUtil.getCurrentDateHourString()));
		Assert.assertTrue(file1.exists());
		Assert.assertEquals(file1, FileSystem.visitDataFile(NAME, DateUtil.getCurrentDateHourString(), 1));
	}

	protected void createFile(File file) throws IOException {
		File parent = file.getParentFile();
		if (!parent.exists() && !parent.mkdirs()) {
			throw new IOException("failed to create file parent directories.");
		}

		if (!file.createNewFile()) {
			throw new IOException("file already exists.");
		}
	}
}
