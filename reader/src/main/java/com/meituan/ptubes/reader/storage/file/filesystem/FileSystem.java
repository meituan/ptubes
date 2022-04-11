package com.meituan.ptubes.reader.storage.file.filesystem;

import com.meituan.ptubes.common.utils.DateUtil;
import com.meituan.ptubes.common.utils.GZipUtil;
import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.common.utils.IOUtils;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.storage.channel.DefaultWriteChannel;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.data.DataManagerFinder;
import com.meituan.ptubes.reader.storage.file.data.read.SingleReadDataManager;
import com.meituan.ptubes.reader.storage.file.index.read.L1ReadIndexManager;
import com.meituan.ptubes.reader.storage.file.index.read.L2ReadIndexManager;
import com.meituan.ptubes.reader.storage.mem.buffer.BinlogInfoFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;


public final class FileSystem {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultWriteChannel.class);

	public static final String TMP = ".tmp";

	public static final String L1_INDEX_PREFIX = "l1Index";

	public static final String L1_INDEX_SUFFIX = ".l1idx";

	public static final String L2_INDEX_PREFIX = "bucket-";

	public static final String L2_INDEX_SUFFIX = ".l2idx";

	public static final String DATA_PREFIX = "bucket-";

	public static final String DATA_SUFFIX = ".data";

	
	public static final String DEFAULT_PATH = ContainerConstants.BASE_DIR;
	public static final String STORAGE_DIR_NAME = "storage";

	public static String l1IndexDir = STORAGE_DIR_NAME + "/index/l1Index/";

	public static String l2IndexDir = STORAGE_DIR_NAME + "/index/l2Index/";

	public static String dataDir = STORAGE_DIR_NAME + "/data/";

	private static String backupDir = "binlogBak";

	private static final String EXPIRED_DIR = STORAGE_DIR_NAME + "/expired";

	private FileSystem() {
	}

	public static String genBasePath(String name) {
		return FilenameUtils.concat(DEFAULT_PATH, name);
	}

	public static String genBackupPath(String name) {
		return FilenameUtils.concat(genBasePath(name), backupDir);
	}

	public static String genExpiredPath(String name) {
		return FilenameUtils.concat(genBasePath(name), EXPIRED_DIR);
	}

	public static String parseDb(File file) {
		return file.getParentFile().getParentFile().getName();
	}

	public static String parseDate(File file) {
		return file.getParentFile().getName();
	}

	public static int parseNumber(File file, String prefix, String suffix) {
		String numberString = StringUtils.substringBetween(file.getName(), prefix, suffix);
		return Integer.valueOf(numberString);
	}

	public static int maxFileNumber(String baseDir, String name, String date, String prefix, String suffix) {
		File[] files = visitFiles(baseDir, name, date, prefix, suffix);
		int max = -1;
		for (File file : files) {
			int number = parseNumber(file, prefix, suffix);
			max = number > max ? number : max;
		}
		return max;
	}

	protected static File visitFile(String baseDir, String name, String date, int number, String prefix,
			String suffix) {
		File databaseDir = new File(genBasePath(name), baseDir);
		File dateDir = new File(databaseDir, date);
		File file = new File(dateDir, genFileName(number, prefix, suffix));

		return file.isFile() && file.exists() ? file : null;
	}

	public static File[] visitFiles(String baseDir, String database, String date, final String prefix,
			final String suffix) {
		File databaseDir = new File(genBasePath(database), baseDir);
		File dateDir = new File(databaseDir, date);
		File[] files = dateDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File file) {
				return file.getName()
					.startsWith(prefix) && (file.getName()
					.endsWith(suffix) || file.getName()
					.endsWith(suffix + GZipUtil.EXT));
			}
		});

		return files == null ? new File[0] : files;
	}

	public static File[] visitL2indexFiles(String name, String date) {
		return visitFiles(l2IndexDir, name, date, L2_INDEX_PREFIX, L2_INDEX_SUFFIX);
	}

	public static File getL1IndexDir() {
		return new File(l1IndexDir);
	}

	public static File getL2IndexDir() {
		return new File(l2IndexDir);
	}

	public static File getDataDir() {
		return new File(dataDir);
	}

	public static File getBackupDir() {
		return new File(backupDir);
	}

	public static File nextL1IndexFile(String physycsSourceName) throws IOException {
		File sourceDir = new File(genBasePath(physycsSourceName), l1IndexDir);
		File file = new File(sourceDir, genL1IndexName());
		createFile(file);
		return file;
	}

	public static File visitL1IndexFile(String physycsSourceName) {
		File sourceDir = new File(genBasePath(physycsSourceName), l1IndexDir);
		File l1Index = new File(sourceDir, genL1IndexName());
		return l1Index.isFile() && l1Index.canRead() && l1Index.canWrite() ? l1Index : null;
	}

	public static void backupL1IndexFile(String readerTaskName) {
		File file = visitL1IndexFile(readerTaskName);
		if (file == null || !file.exists()) {
			return;
		}
		File sourceDir = new File(genBasePath(readerTaskName), l1IndexDir);
		File l1IndexTmp = new File(sourceDir, genL1IndexTmpName());
		file.renameTo(l1IndexTmp);
	}

	public static File[] visitL2IndexDateDirs(String physycsSourceName) {
		return visitDateDirs(genBasePath(physycsSourceName), l2IndexDir);
	}

	public static File visitL2IndexFile(String physycsSourceName, String date, int number) {
		return visitFile(l2IndexDir, physycsSourceName, date, number, L2_INDEX_PREFIX, L2_INDEX_SUFFIX);
	}

	public static File visitMaxL2IndexFile(String physycsSourceName) {
		File[] dirs = visitL2IndexDateDirs(physycsSourceName);
		int date = -1;
		for (File file : dirs) {
			int currDate = Integer.parseInt(file.getName());
			date = currDate > date ? currDate : date;
		}
		File[] files = visitL2indexFiles(physycsSourceName, date + "");
		int maxBucketNum = -1;
		for (File file : files) {
			int currNum = parseNumber(file, L2_INDEX_PREFIX, L2_INDEX_SUFFIX);
			maxBucketNum = currNum > maxBucketNum ? currNum : maxBucketNum;
		}
		return visitFile(l2IndexDir, physycsSourceName, date + "", maxBucketNum, L2_INDEX_PREFIX, L2_INDEX_SUFFIX);
	}

	public static File nextL2IndexFile(String physycsSourceName, String dateStr) throws IOException {
		int max = maxL2IndexFileNumber(physycsSourceName, dateStr);
		return createFile(l2IndexDir, physycsSourceName, dateStr, max + 1, L2_INDEX_PREFIX, L2_INDEX_SUFFIX);
	}

	public static File[] visitDataDateDirs(String physycsSourceName) {
		return visitDateDirs(genBasePath(physycsSourceName), dataDir);
	}

	public static File visitDataDateDir(String physycsSourceName, String date) {
		return new File(genBasePath(physycsSourceName), dataDir + date);
	}

	public static File visitIndexDateDir(String physycsSourceName, String date) {
		return new File(genBasePath(physycsSourceName), l2IndexDir + date);
	}

	public static File[] visitDataFiles(String physycsSourceName, String date) {
		return visitFiles(dataDir, physycsSourceName, date, DATA_PREFIX, DATA_SUFFIX);
	}

	public static File visitDataFile(
		String readerTaskName,
		String date,
		int number
	) {
		File file = visitFile(
			dataDir,
			readerTaskName,
			date,
			number,
			DATA_PREFIX,
			DATA_SUFFIX
		);
		if (file == null) {
			return visitFile(
				dataDir,
				readerTaskName,
				date,
				number,
				DATA_PREFIX,
				DATA_SUFFIX + GZipUtil.EXT
			);
		} else {
			return file;
		}
	}

	public static File visitMaxDataFile(String physycsSourceName) {
		File[] dirs = visitDataDateDirs(physycsSourceName);
		int date = -1;
		for (File file : dirs) {
			int currDate = Integer.parseInt(file.getName());
			date = currDate > date ? currDate : date;
		}
		File[] files = visitDataFiles(physycsSourceName, date + "");
		int maxBucketNum = -1;
		for (File file : files) {
			int currNum = parseNumber(file, DATA_PREFIX, DATA_SUFFIX);
			maxBucketNum = currNum > maxBucketNum ? currNum : maxBucketNum;
		}
		return visitFile(dataDir, physycsSourceName, date + "", maxBucketNum, DATA_PREFIX, DATA_SUFFIX);
	}

	/**
	 * Construct the next bucket file in the dateStr directory
	 * @param physycsSourceName reader task name
	 * @param dateStr
	 * @return
	 * @throws IOException
	 */
	public static File nextDataFile(String physycsSourceName, String dateStr) throws IOException {
		int max = maxFileNumber(physycsSourceName, dateStr);
		return createFile(dataDir, physycsSourceName, dateStr, max + 1, DATA_PREFIX, DATA_SUFFIX);
	}

	protected static String genL1IndexName() {
		return L1_INDEX_PREFIX + L1_INDEX_SUFFIX;
	}

	protected static String genL1IndexTmpName() {
		return L1_INDEX_PREFIX + L1_INDEX_SUFFIX + TMP;
	}

	protected static File[] visitDatabaseDirs(String baseDir) {
		File[] files = new File(baseDir).listFiles();

		return files == null ? new File[0] : files;
	}

	protected static File[] visitDateDirs(String physycsSourceName, String baseDir) {
		File databaseDir = new File(physycsSourceName, baseDir);
		File[] files = databaseDir.listFiles();

		return files == null ? new File[0] : files;
	}

	protected static String genFileName(int number, String prefix, String suffix) {
		return prefix + number + suffix;
	}

	public static int maxL2IndexFileNumber(String physycsSourceName, String date) {
		return maxFileNumber(l2IndexDir, physycsSourceName, date, L2_INDEX_PREFIX, L2_INDEX_SUFFIX);
	}

	public static int maxFileNumber(String physycsSourceName, String date) {
		return maxFileNumber(dataDir, physycsSourceName, date, DATA_PREFIX, DATA_SUFFIX);
	}

	protected static void createFile(File file) throws IOException {
		File parent = file.getParentFile();
		if (!parent.exists() && !parent.mkdirs()) {
			throw new IOException("failed to create file parent directories.");
		}

		if (!file.createNewFile()) {
			throw new IOException("file already exists.");
		}
	}

	protected static File createFile(String baseDir, String physycsSourceName, String date, int number, String prefix,
			String suffix) throws IOException {
		File databaseDir = new File(genBasePath(physycsSourceName), baseDir);
		File dateDir = new File(databaseDir, date);
		File file = new File(dateDir, genFileName(number, prefix, suffix));

		File parent = file.getParentFile();
		if (!parent.exists() && !parent.mkdirs()) {
			throw new IOException("failed to create file parent directories.");
		}

		if (!file.createNewFile()) {
			throw new IOException("file already exists.");
		}

		return file;
	}

	public static String parseDataDate(File file) {
		return parseDate(file);
	}

	public static int parseDataNumber(File file) {
		return parseNumber(file, DATA_PREFIX, DATA_SUFFIX);
	}

	public static String parseL2IndexDate(File file) {
		return parseDate(file);
	}

	public static int parseL2IndexNumber(File file) {
		return parseNumber(file, L2_INDEX_PREFIX, L2_INDEX_SUFFIX);
	}

	
	public static BinlogInfo getMinBinlogInfo(StorageConfig storageConfig, SourceType sourceType) {
		File file = visitL1IndexFile(storageConfig.getReaderTaskName());
		if (file == null || !file.exists()) {
			return null;
		}
		L1ReadIndexManager l1ReadIndexManager = new L1ReadIndexManager(file,
				storageConfig.getFileConfig().getL1ReadBufSizeInByte(),
				storageConfig.getFileConfig().getL1ReadAvgSizeInByte(), storageConfig.getIndexPolicy(), sourceType);
		l1ReadIndexManager.start();
		Pair<BinlogInfo, DataPosition> firstIndexEntry = null;
		try {
			while(true) {
				firstIndexEntry = l1ReadIndexManager.next();
				if (firstIndexEntry == null) {
					return null;
				}
				File l2IndexFile = visitL2IndexFile(storageConfig.getReaderTaskName(), firstIndexEntry.getValue().getCreationDate() + "", firstIndexEntry.getValue().getBucketNumber());
				if (l2IndexFile != null && l2IndexFile.exists()) {
					return firstIndexEntry.getKey();
				}
			}
		} catch (IOException e) {
			return null;
		} finally {
			l1ReadIndexManager.stop();
		}
	}

	public static BinlogInfo getMaxBinlogInfo(StorageConfig storageConfig, SourceType sourceType) throws IOException {
		File file = visitMaxL2IndexFile(storageConfig.getReaderTaskName());
		if (file == null || !file.exists()) {
			return null;
		}
		L2ReadIndexManager l2ReadIndexManager = new L2ReadIndexManager(file,
				storageConfig.getFileConfig().getL1ReadBufSizeInByte(),
				storageConfig.getFileConfig().getL1ReadAvgSizeInByte(), storageConfig.getIndexPolicy(), sourceType);
		l2ReadIndexManager.start();
		Pair<BinlogInfo, DataPosition> latestIndexEntry = l2ReadIndexManager.findLatest();
		l2ReadIndexManager.stop();
		File dataFile = visitDataFile(storageConfig.getReaderTaskName(), latestIndexEntry.getValue().getCreationDate() + "", latestIndexEntry.getValue().getBucketNumber());
		if (dataFile == null || !file.exists()) {
			return null;
		}
		SingleReadDataManager singleReadDataManager = new SingleReadDataManager(dataFile, storageConfig.getFileConfig().getDataReadBufSizeInByte(), storageConfig.getFileConfig().getDataReadAvgSizeInByte());
		singleReadDataManager.start();
		singleReadDataManager.open(latestIndexEntry.getValue());
		PtubesEvent event = singleReadDataManager.next();
		BinlogInfo maxBinlogInfo = event.getBinlogInfo();
		while (event.getEventType() != EventType.NO_MORE_EVENT) {
			try {
				event = singleReadDataManager.next();
				maxBinlogInfo = event.getBinlogInfo();
			} catch (EOFException e) {
				break;
			}
		}
		return maxBinlogInfo;
	}

	public static void recover(StorageConfig storageConfig, SourceType sourceType) throws IOException {
		File[] dirs = FileSystem.visitL2IndexDateDirs(storageConfig.getReaderTaskName());
		if (dirs == null || dirs.length <= 0) {
			// The service is started for the first time, no data has been written before
			return;
		}
		File maxL2IndexFile = FileSystem.visitMaxL2IndexFile(storageConfig.getReaderTaskName());
		File maxDataFile = FileSystem.visitMaxDataFile(storageConfig.getReaderTaskName());
		RandomAccessFile dataFile = null;
		RandomAccessFile indexFile = null;
		try {
			dataFile = new RandomAccessFile(maxDataFile.getAbsolutePath(), "rw");
			indexFile = new RandomAccessFile(maxL2IndexFile.getAbsolutePath(), "rw");
		} catch (FileNotFoundException e) {
			IOUtils.closeQuietly(dataFile);
			IOUtils.closeQuietly(indexFile);
			return;
		}
		FileChannel dataFc = dataFile.getChannel();
		long dataFileSize = dataFc.size();

		FileChannel indexFc = indexFile.getChannel();
		long indexFileSize = indexFc.size();

		L2ReadIndexManager l2ReadIndexManager = null;
		try {
			// If the index file is damaged, remove the damaged part
			long brokenSize = indexFileSize % (BinlogInfoFactory.getLength(sourceType) + DataPosition.getSizeInByte() + 4);
			if (brokenSize != 0) {
				long remainSize = indexFileSize - brokenSize;
				LOG.info(
						"L2Index File: " + maxL2IndexFile.getName() + ", length: " + indexFileSize + " truncate to "
								+ remainSize);
				indexFc.truncate(remainSize);
				indexFc.force(true);
				if (remainSize == 0) {
					LOG.info("Delete l2index: " + maxL2IndexFile.getName());
					maxL2IndexFile.delete();
					recover(storageConfig, sourceType);
					return;
				}
			}

			int date = Integer.parseInt(FileSystem.parseDate(maxDataFile));
			int bucketNumber = FileSystem.parseNumber(maxDataFile, FileSystem.DATA_PREFIX, FileSystem.DATA_SUFFIX);
			l2ReadIndexManager = new L2ReadIndexManager(maxL2IndexFile,
					storageConfig.getFileConfig().getL2ReadBufSizeInByte(),
					storageConfig.getFileConfig().getL2ReadAvgSizeInByte(), storageConfig.getIndexPolicy(), sourceType);
			l2ReadIndexManager.start();
			Pair<BinlogInfo, DataPosition> prevIndexEntry = null;
			int prevIndexPosition = 0;
			Pair<BinlogInfo, DataPosition> indexEntry = null;
			int indexPosition = 0;
			boolean foundBucket = false;
			while (true) {
				try {
					Pair<BinlogInfo, DataPosition> currIndexEntry = l2ReadIndexManager.next();
					if (currIndexEntry.getRight().getCreationDate() == date
							&& currIndexEntry.getRight().getBucketNumber() == bucketNumber) {
						foundBucket = true;
						if (currIndexEntry.getRight().getOffset() >= dataFileSize) {
							LOG.info("L2Index File: " + maxL2IndexFile.getName() + ", found bigger binlogInfo");
							break;
						}

					}
					prevIndexEntry = indexEntry;
					prevIndexPosition = indexPosition;
					indexPosition = l2ReadIndexManager.position();
					indexEntry = currIndexEntry;
				} catch (Exception e) {
					LOG.info("L2Index File: " + maxL2IndexFile.getName() + ", arrive end");
					break;
				}
			}

			if (!foundBucket) {
				LOG.info("Delete data: " + maxDataFile.getName());
				maxDataFile.delete();
				recover(storageConfig, sourceType);
				return;
			}

			if (indexEntry == null) {
				// Theoretically will not enter here, because the l2index file cannot be empty
				return;
			}

			dataFc.truncate(indexEntry.getValue().getOffset());
			LOG.info("Data File: " + maxDataFile.getName() + ", length: " + dataFileSize + " truncate to "
					+ indexEntry.getValue());
			dataFc.force(true);
			indexFc.truncate(prevIndexPosition);
			LOG.info("L2Index File: " + maxL2IndexFile.getName() + ", length: " + indexFileSize
					+ " truncate to " + prevIndexPosition);
			indexFc.force(true);

			if(dataFc.size() == 0) {
				LOG.info("Delete Data File: " + maxDataFile.getName());
				maxDataFile.delete();
				recover(storageConfig, sourceType);
				return;
			}
			if(indexFc.size() == 0) {
				LOG.info("Delete L2Index File: " + maxL2IndexFile.getName());
				maxL2IndexFile.delete();
				recover(storageConfig, sourceType);
				return;
			}
		} finally {
			dataFc.close();
			indexFc.close();
			if (l2ReadIndexManager != null) {
				l2ReadIndexManager.stop();
			}
		}
	}

	
	public static void backupStorageDir(String readerTaskName) {
		String srcBasePath = genBasePath(readerTaskName);
		String backupPath = genBackupPath(readerTaskName);
		File srcFile = new File(srcBasePath, STORAGE_DIR_NAME);
		File destDirFile = new File(backupPath);
		File destFile = new File(backupPath, STORAGE_DIR_NAME);
		if (!srcFile.exists()) {
			return;
		}
		if (destFile.exists()) {
			try {
				FileUtils.deleteDirectory(destFile);
			} catch (IOException e) {
				LOG.error(
					"delete backup dir error, readeraTaskName: {}",
					readerTaskName,
					e
				);
			}
		}
		try {
			// The first call to moveDirectory will copy the file. As long as the binlogBak directory exists, renameTo requires the existence of the parent path to succeed.
			if (!destDirFile.exists() || !destDirFile.isDirectory()) {
				destDirFile.mkdirs();
			}
			FileUtils.moveDirectory(srcFile, destFile);
		} catch (IOException e) {
			LOG.error(
				"backup storage dir error, readeraTaskName: {}",
				readerTaskName,
				e
			);
		}
	}

	
	public static void expireL2IndexDir(String dateHour, String readTaskName) {
		File dateDir = visitIndexDateDir(readTaskName, dateHour);
		expireDir(dateDir, readTaskName, L2_INDEX_SUFFIX);
	}

	
	public static void expireDataDir(String dateHour, String readTaskName) {
		File dateDir = FileSystem.visitDataDateDir(readTaskName, dateHour);
		expireDir(dateDir, readTaskName, DATA_SUFFIX);
	}

	public static void clearExpireDir(String readTaskName) {
		File expireDir = new File(genExpiredPath(readTaskName));

		if (expireDir.exists()) {
			try {
				FileUtils.deleteDirectory(expireDir);
			} catch (IOException e) {
				LOG.error("delete dir: {} error", expireDir.getAbsolutePath(), e);
			}
		}
		expireDir.mkdirs();
	}

	private static void expireDir(File file, String readTaskName, String suffix) {
		if (file == null || !file.exists()) {
			return;
		}
		File toDir = new File(genExpiredPath(readTaskName));

		File toFile = new File(toDir, file.getName() + suffix);

		LOG.info("{} rename to {}", file.getAbsolutePath(), toFile.getAbsolutePath());
		file.renameTo(toFile);
	}

	
	public static boolean l2IndexExist(String readerTaskName, int date, int bucketNum) {
		File file = visitL2IndexFile(readerTaskName, date + "", bucketNum);
		return file != null && file.exists();
	}

	
	public static boolean l1IndexExist(String readerTaskName) {
		File file = visitL1IndexFile(readerTaskName);
		return file != null && file.exists() && file.length() > 0;
	}

	// Considering performance issues, all files will not be locked for the time being, so there may still be concurrency issues. It is possible that after checking the reference, the file will be referenced
	public static void retentionFile(
		String readerTaskName,
		int retentionHours
	) {
		if (retentionHours < 1) {
			retentionHours = 1;
		}
		try {
			FileSystem.clearExpireDir(readerTaskName);
			File[] dataFileDates = FileSystem.visitDataDateDirs(readerTaskName);
			List<String> dateDirs = new ArrayList<>();
			Arrays.asList(dataFileDates)
				.stream()
				.forEach(file -> dateDirs.add(file.getName()));
			Collections.sort(dateDirs);

			List<String> needClearHours = new ArrayList<>();
			for (String dateHour : dateDirs) {
				if (!DateUtil.isExpired(
					dateHour,
					retentionHours
				)) {
					break;
				}
				File[] dataFiles = FileSystem.visitDataFiles(
					readerTaskName,
					dateHour
				);
				boolean canClean = true;
				for (File file : dataFiles) {
					if (DataManagerFinder.getRefCount(file.getAbsolutePath()) > 0) {
						LOG.warn(
							"Can not retention file: {}, refCount: {}",
							file.getAbsolutePath(),
							DataManagerFinder.getRefCount(file.getAbsolutePath())
						);
						canClean = false;
						break;
					}
				}
				if (canClean) {
					needClearHours.add(dateHour);
				} else {
					break;
				}
			}

			if (needClearHours.isEmpty()) {
				return;
			}

			for (String dateHour : needClearHours) {
				FileSystem.expireL2IndexDir(
					dateHour,
					readerTaskName
				);
				LOG.info(
					"Task: {}, expire L2Index: {} success",
					readerTaskName,
					dateHour
				);
				FileSystem.expireDataDir(
					dateHour,
					readerTaskName
				);
				LOG.info(
					"Task: {}, expire data: {} success",
					readerTaskName,
					dateHour
				);
			}
		} catch (Exception e) {
			LOG.error(
				"Retention file error, taskName: {}",
				readerTaskName,
				e
			);
		}
	}

	public static void compressFile(
		String readerTaskName,
		int compressHours
	) {
		if (compressHours < 0) {
			return;
		}

		if (compressHours < 1) {
			compressHours = 1;
		}
		try {
			File[] dataFileDates = FileSystem.visitDataDateDirs(readerTaskName);
			List<String> dateDirs = new ArrayList<>();
			Arrays.asList(dataFileDates)
				.stream()
				.forEach(file -> dateDirs.add(file.getName()));
			Collections.sort(dateDirs);

			List<String> needCompressHours = new ArrayList<>();
			for (String dateHour : dateDirs) {
				if (!DateUtil.isExpired(
					dateHour,
					compressHours
				)) {
					break;
				}
				File[] dataFiles = FileSystem.visitDataFiles(
					readerTaskName,
					dateHour
				);
				boolean canCompress = true;
				for (File file : dataFiles) {
					if (DataManagerFinder.getRefCount(file.getAbsolutePath()) > 0) {
						LOG.warn(
							"Can not compress file: {}, refCount: {}",
							file.getAbsolutePath(),
							DataManagerFinder.getRefCount(file.getAbsolutePath())
						);
						canCompress = false;
						break;
					}
				}
				if (canCompress) {
					needCompressHours.add(dateHour);
				} else {
					break;
				}
			}

			if (needCompressHours.isEmpty()) {
				return;
			}

			for (String dateHour : needCompressHours) {
				File[] dataFiles = visitDataFiles(readerTaskName, dateHour);
				for (File file : dataFiles) {
					if (!file.getAbsolutePath().endsWith(GZipUtil.EXT)) {
						GZipUtil.compress(file, true);
					}
				}
				LOG.info(
					"Task: {}, compress data file: {} success",
					readerTaskName,
					dateHour
				);
			}
		} catch (Exception e) {
			LOG.error(
				"Compress data file error, taskName: {}",
				readerTaskName,
				e
			);
		}
	}
}
