package com.meituan.ptubes.reader.schema.service;

import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.SchemaNotFoundException;
import com.meituan.ptubes.common.exception.SchemaParseException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.schema.common.SchemaVersion;
import com.meituan.ptubes.reader.schema.common.TableMeta;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;
import com.meituan.ptubes.reader.schema.util.SchemaGenerateUtil;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;

public class FileBasedISchemaService implements ISchemaService {

	private static final Logger LOG = LoggerFactory.getLogger(FileBasedISchemaService.class);

	public static final String SCHEMA_FILE_SUFFIX = ".dtsc";
	public static final String CLEAN_MARKED_SCHEMA_FILE_SUFFIX = ".delete";
	public static final String SCHEMA_FILE_NAME_FORMAT = "%s.%d" + SCHEMA_FILE_SUFFIX;
	public static final Pattern SCHEMA_FILE_PATTERN = Pattern.compile("(.*)\\.(\\d+)" + SCHEMA_FILE_SUFFIX);

	private final File baseDirFile;
	private final ReentrantReadWriteLock rwLock;
	private final ReentrantReadWriteLock.ReadLock readLock;
	private final ReentrantReadWriteLock.WriteLock writeLock;

	private long schemaFileExpiredTime = ContainerConstants.SCHEMA_FILE_EXPIRED_TIME;
	private long cleanMarkSchemaFileExpiredTime = ContainerConstants.CLEAN_MARK_SCHEMA_FILE_EXPIRED_TIME;

	/**
	 *
	 * @param baseDirPath
	 */
	public FileBasedISchemaService(String baseDirPath) throws PtubesException {
		this.baseDirFile = new File(baseDirPath);
		if (this.baseDirFile.exists() == false) {
			this.baseDirFile.mkdirs();
		} else if (this.baseDirFile.isDirectory() == false) {
			// TODO: throw an exception or delete this file and create dir?
			throw new PtubesException("schema dir not found: " + baseDirPath);
		}
		this.rwLock = new ReentrantReadWriteLock();
		this.readLock = rwLock.readLock();
		this.writeLock = rwLock.writeLock();
	}

	private void checkVersion(SchemaVersion schemaVersion) throws IllegalArgumentException {
		if (schemaVersion.getVersion() <= 0) {
			throw new IllegalArgumentException("illegal schema version " + schemaVersion.toString());
		}
	}

	private SchemaVersion parseSchemaVersion(String fileName) {
		Matcher matcher = SCHEMA_FILE_PATTERN.matcher(fileName);
		if (matcher.matches()) {
			String baseName = matcher.group(1);
			int version = Integer.parseInt(matcher.group(2));
			return new SchemaVersion(baseName, version);
		} else {
			LOG.warn("Invalid file name: " + fileName);
			return null;
		}
	}

	private String reverseParseSchemaFileName(SchemaVersion schemaVersion) {
		String table = schemaVersion.getTableFullName();
		int version = schemaVersion.getVersion();
		return String.format(SCHEMA_FILE_NAME_FORMAT, table, version);
	}

	@Override public VersionedSchema parseSchemaFile(File schemaFile) throws IOException, SchemaParseException {
		SchemaVersion schemaVersion = parseSchemaVersion(schemaFile.getName());
		InputStream schemaStream = new FileInputStream(schemaFile);
		try {
			String schemaString = IOUtils.toString(schemaStream);
			TableMeta tableMeta = SchemaGenerateUtil.parseRawSchema(schemaString);
			return new VersionedSchema(schemaString, tableMeta, schemaVersion);
		} finally {
			IOUtils.closeQuietly(schemaStream);
		}
	}

	@Override
	public TreeMap<SchemaVersion, File> tableSortedVersionSchemaMap(String table) throws InterruptedException {
		TreeMap<SchemaVersion, File> ans = new TreeMap<>(new Comparator<SchemaVersion>() {
			@Override
			public int compare(SchemaVersion o1, SchemaVersion o2) {
				return o1.getVersion() - o2.getVersion();
			}
		});

		File[] tableSchemaFiles = new File[0];
		this.readLock.lockInterruptibly();
		try {
			tableSchemaFiles = baseDirFile.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					if (pathname.isFile()) {
						String fileName = pathname.getName();
						return fileName.endsWith(SCHEMA_FILE_SUFFIX) && fileName.startsWith(table);
					}

					return false;
				}
			});
		} finally {
			this.readLock.unlock();
		}

		for (File schemaFile : tableSchemaFiles) {
			SchemaVersion schemaVersion = parseSchemaVersion(schemaFile.getName());
			ans.put(schemaVersion, schemaFile);
		}

		return ans;
	}

	@Override
	public Map<String, TreeMap<SchemaVersion, File>> tableSortedVersionSchemaMap(Set<String> tables, boolean removeIfRequire) throws InterruptedException {
		Map<String, TreeMap<SchemaVersion, File>> tableSchemaVersionMap = new HashMap<>((int)(tables.size() * 1.34));

		this.readLock.lockInterruptibly();
		try {
			File[] fileInDir = baseDirFile.listFiles();
			for (File schemaFile : fileInDir) {
				if (schemaFile.isFile() == false) {
					continue;
				}

				String schemaFileName = schemaFile.getName();
				if (schemaFileName.endsWith(SCHEMA_FILE_SUFFIX) == false) {
					if (removeIfRequire) {
						schemaFile.delete();
					}
					continue;
				}

				SchemaVersion schemaVersion = parseSchemaVersion(schemaFileName);// version number
				if (schemaVersion != null) {
					String table = schemaVersion.getTableFullName();
					if (tables.contains(table) == false) {
						if (removeIfRequire) {
							schemaFile.delete();
						}
						continue;
					}
					TreeMap<SchemaVersion, File> tableSchemaVersions = tableSchemaVersionMap.get(table);
					if (tableSchemaVersions == null) {
						tableSchemaVersions = new TreeMap<>(new SchemaVersion.SchemaVersionComparator());
						tableSchemaVersionMap.put(table, tableSchemaVersions);
					}
					tableSchemaVersions.put(schemaVersion, schemaFile);
				}
			}
		} finally {
			this.readLock.unlock();
		}

		return tableSchemaVersionMap;
	}

	@Override
	public Map<String, Integer> tableVersion(Set<String> tables) throws InterruptedException {
		Map<String, Integer> ans = new HashMap<>();

		this.readLock.lockInterruptibly();
		try {
			Map<String, TreeMap<SchemaVersion, File>> tableSchemaVersionMap = tableSortedVersionSchemaMap(tables, false);
			for (Map.Entry<String, TreeMap<SchemaVersion, File>> entry : tableSchemaVersionMap.entrySet()) {
				String table = entry.getKey();
				TreeMap<SchemaVersion, File> tablesSchemaVersionSet = entry.getValue();
				Map.Entry<SchemaVersion, File> schemaFileEntry = tablesSchemaVersionSet.lastEntry();
				SchemaVersion schemaVersion = schemaFileEntry.getKey();
				ans.put(table, schemaVersion.getVersion());
			}
		} finally {
			this.readLock.unlock();
		}

		return ans;
	}

	@Override
	public Map<String, VersionedSchema> fetchLatestTableSchema(Set<String> tables) throws IOException, SchemaParseException, InterruptedException {
		Map<String, VersionedSchema> ans = new HashMap<String, VersionedSchema>((int)(tables.size() * 1.34));

		this.readLock.lockInterruptibly();
		try {
			Map<String, TreeMap<SchemaVersion, File>> tableSchemaVersionMap = tableSortedVersionSchemaMap(tables, false);

			for (String table : tableSchemaVersionMap.keySet()) {
				TreeMap<SchemaVersion, File> tableSchemaVersionSet = tableSchemaVersionMap.get(table);
				SchemaVersion schemaVersion = tableSchemaVersionSet.lastKey();
				File latestVersionFile = tableSchemaVersionSet.get(schemaVersion);
				InputStream schemaStream = new FileInputStream(latestVersionFile);
				try {
					String schemaString = IOUtils.toString(schemaStream);
					TableMeta tableMeta = SchemaGenerateUtil.parseRawSchema(schemaString);
					ans.put(table, new VersionedSchema(schemaString, tableMeta, schemaVersion));
				} finally {
					IOUtils.closeQuietly(schemaStream);
				}
			}
		} finally {
			this.readLock.unlock();
		}

		return ans;
	}

	@Override
	public Map<String, VersionedSchema> fetchTableSchemaByTime(Set<String> tables, long time) throws IOException, SchemaParseException, InterruptedException {
		Map<String, VersionedSchema> ans = new HashMap<>();

		this.readLock.lockInterruptibly();
		try {
			Map<String, TreeMap<SchemaVersion, File>> tableSortedSchemaFiles = tableSortedVersionSchemaMap(tables, false);

			for (Map.Entry<String, TreeMap<SchemaVersion, File>> entry : tableSortedSchemaFiles.entrySet()) {
				String table = entry.getKey();
				TreeMap<SchemaVersion, File> schemaFiles = entry.getValue();
				NavigableSet<SchemaVersion> schemaVersionSet = schemaFiles.descendingKeySet();
				Iterator<SchemaVersion> iterator = schemaVersionSet.descendingIterator();
				while (iterator.hasNext()) {
					SchemaVersion schemaVersion = iterator.next();
					File schemaFile = schemaFiles.get(schemaVersion);
					if (schemaFile.lastModified() <= time) {
						InputStream schemaStream = new FileInputStream(schemaFile);
						try {
							String schemaString = IOUtils.toString(schemaStream);
							TableMeta tableMeta = SchemaGenerateUtil.parseRawSchema(schemaString);
							ans.put(table, new VersionedSchema(schemaString, tableMeta, schemaVersion));
						} finally {
							IOUtils.closeQuietly(schemaStream);
						}
					}
				}
			}
		} finally {
			this.readLock.unlock();
		}

		return ans;
	}

	@Override
	public Map<String, TreeMap<SchemaVersion, VersionedSchema>> fetchAllTableSchemas(Set<String> tables) throws IOException, SchemaParseException, InterruptedException {
		Map<String, TreeMap<SchemaVersion, VersionedSchema>> ans = new HashMap<>((int)(tables.size() * 1.34));

		this.readLock.lockInterruptibly();
		try {
			File[] fileInDir = baseDirFile.listFiles();
			for (File schemaFile : fileInDir) {
				if (schemaFile.isFile() == false) {
					continue;
				}

				String schemaFileName = schemaFile.getName();
				if (schemaFileName.endsWith(SCHEMA_FILE_SUFFIX)) {
					SchemaVersion schemaVersion = parseSchemaVersion(schemaFileName);
					if (schemaVersion != null) {
						String table = schemaVersion.getTableFullName();
						if (tables.contains(table) == false) {
							continue;
						}

						TreeMap<SchemaVersion, VersionedSchema> tableSchemaVersions = ans.get(table);
						if (tableSchemaVersions == null) {
							tableSchemaVersions = new TreeMap<>((o1, o2) -> o1.getVersion() - o2.getVersion());
							ans.put(table, tableSchemaVersions);
						}

						InputStream schemaStream = new FileInputStream(schemaFile);
						try {
							String schemaString = IOUtils.toString(schemaStream);
							TableMeta tableMeta = SchemaGenerateUtil.parseRawSchema(schemaString);
							tableSchemaVersions.put(schemaVersion, new VersionedSchema(schemaString, tableMeta, schemaVersion));
						} finally {
							IOUtils.closeQuietly(schemaStream);
						}
					}
				} else {
				}
			}
		} finally {
			this.readLock.unlock();
		}
		return ans;
	}

	@Override
	public Map<String, TreeMap<SchemaVersion, VersionedSchema>> fetchPartialTableSchemas(Set<String> tables, int maxCacheNum) throws IOException, SchemaParseException, InterruptedException {
		Map<String, TreeMap<SchemaVersion, VersionedSchema>> ans = new HashMap<>((int)(tables.size() * 1.34));

		this.readLock.lockInterruptibly();
		try {
			Map<String, TreeMap<SchemaVersion, File>> tableSchemaVersionMap = tableSortedVersionSchemaMap(tables, false);

			for (String table : tableSchemaVersionMap.keySet()) {
				TreeMap<SchemaVersion, File> tableSchemaVersionSet = tableSchemaVersionMap.get(table);
				TreeMap<SchemaVersion, VersionedSchema> tableSchemaMap = ans.get(table);
				if (tableSchemaMap == null) {
					tableSchemaMap = new TreeMap<>(new Comparator<SchemaVersion>() {
						@Override
						public int compare(SchemaVersion o1, SchemaVersion o2) {
							return o1.getVersion() - o2.getVersion();
						}
					});
					ans.put(table, tableSchemaMap);
				}

				int counter = 0;
				Iterator<SchemaVersion> versionIterator = tableSchemaVersionSet.descendingKeySet().iterator();
				while (versionIterator.hasNext() && counter < maxCacheNum) {
					SchemaVersion schemaVersion = versionIterator.next();
					File schemaFile = tableSchemaVersionSet.get(schemaVersion);
					InputStream schemaStream = new FileInputStream(schemaFile);
					try {
						String schemaString = IOUtils.toString(schemaStream);
						TableMeta tableMeta = SchemaGenerateUtil.parseRawSchema(schemaString);
						tableSchemaMap.put(schemaVersion, new VersionedSchema(schemaString, tableMeta, schemaVersion));
					} finally {
						IOUtils.closeQuietly(schemaStream);
					}
					counter ++;
				}
			}
		} finally {
			this.readLock.unlock();
		}
		return ans;
	}

	@Override
	public VersionedSchema loadLatestTableSchema(String tableFullName) throws InterruptedException,
		SchemaNotFoundException, SchemaParseException {
		this.readLock.lockInterruptibly();
		try {
			TreeMap<SchemaVersion, File> tableSortedSchemaFiles = tableSortedVersionSchemaMap(tableFullName);
			if (tableSortedSchemaFiles.isEmpty()) {
				throw new SchemaNotFoundException("no schema file for table " + tableFullName);
			}

			Map.Entry<SchemaVersion, File> latestSchemaFile = tableSortedSchemaFiles.lastEntry();
			SchemaVersion schemaVersion = latestSchemaFile.getKey();
			File schemaFile = latestSchemaFile.getValue();
			InputStream schemaStream = null;
			try {
				schemaStream = new FileInputStream(schemaFile);
				String schemaString = IOUtils.toString(schemaStream);
				TableMeta tableMeta = SchemaGenerateUtil.parseRawSchema(schemaString);
				return new VersionedSchema(schemaString, tableMeta, schemaVersion);
			} catch (IOException ioe) {
				LOG.error("load schema {} error", schemaVersion.toString(), ioe);
				throw new SchemaNotFoundException("load schema " + schemaVersion.toString() + " error", ioe);
			} finally {
				if (schemaStream != null) {
					IOUtils.closeQuietly(schemaStream);
				}
			}
		} finally {
			this.readLock.unlock();
		}
	}

	@Override
	public VersionedSchema loadTableSchemaByTime(String tableFullName, long timestamp) throws InterruptedException, SchemaNotFoundException, SchemaParseException {
		TreeMap<SchemaVersion, File> tableSortedSchemaFiles = tableSortedVersionSchemaMap(tableFullName);

		NavigableSet<SchemaVersion> sortedVersionSet = tableSortedSchemaFiles.descendingKeySet();
		Iterator<SchemaVersion> versionIterator = sortedVersionSet.descendingIterator();
		while (versionIterator.hasNext()) {
			SchemaVersion schemaVersion = versionIterator.next();
			File schemaFile = tableSortedSchemaFiles.get(schemaVersion);
			if (schemaFile.lastModified() <= timestamp) {
				InputStream schemaStream = null;
				try {
					schemaStream = new FileInputStream(schemaFile);
					String schemaString = IOUtils.toString(schemaStream);
					TableMeta tableMeta = SchemaGenerateUtil.parseRawSchema(schemaString);
					return new VersionedSchema(schemaString, tableMeta, schemaVersion);
				} catch (IOException ioe) {
					LOG.error("load schema {} error", schemaVersion.toString(), ioe);
					throw new SchemaNotFoundException("load schema " + schemaVersion.toString() + " error", ioe);
				} finally {
					if (schemaStream != null) {
						IOUtils.closeQuietly(schemaStream);
					}
				}
			}
		}

		throw new SchemaNotFoundException("no schema which is earler than " + timestamp + " for table " + tableFullName);
	}

	@Override
	public VersionedSchema loadTableSchemaByVersion(SchemaVersion schemaVersion) throws InterruptedException, SchemaNotFoundException, SchemaParseException {
		checkVersion(schemaVersion);

		String schemaFileName = reverseParseSchemaFileName(schemaVersion);
		this.readLock.lockInterruptibly();
		try {
			File schemaFile = new File(baseDirFile, schemaFileName);
			InputStream schemaStream = null;
			try {
				schemaStream = new FileInputStream(schemaFile);
				String schemaString = IOUtils.toString(schemaStream);
				TableMeta tableMeta = SchemaGenerateUtil.parseRawSchema(schemaString);
				return new VersionedSchema(schemaString, tableMeta, schemaVersion);
			}  catch (IOException ioe) {
				LOG.error("load schema {} error", schemaVersion.toString(), ioe);
				throw new SchemaNotFoundException("load schema " + schemaVersion.toString() + " error", ioe);
			} finally {
				if (schemaStream != null) {
					IOUtils.closeQuietly(schemaStream);
				}
			}
		} finally {
			this.readLock.unlock();
		}
	}

	@Override
	public File saveTableSchema(SchemaVersion schemaVersion, String schema) throws IOException, InterruptedException {
		this.writeLock.lockInterruptibly();
		PrintWriter pw = null;
		try {
			String targetFileName = reverseParseSchemaFileName(schemaVersion);
			File targetFile = new File(baseDirFile, targetFileName);
			pw = new PrintWriter(new FileWriter(targetFile));
			pw.println(schema);
			pw.flush();
			// attention: the file may not be placed immediately
			return targetFile;
		} finally {
			this.writeLock.unlock();
			if (pw != null) {
				pw.close();
			}
		}
	}

	// There is a conflict between cleaning the schema and fetch, get, and save operations, so read-write locks are also required. The schema that has read the memory will not be affected by the cleaning. Be careful to keep the schema within 30 days or at least 2 versions
	@Override
	public void markCleanFlagOnSchemaFile() throws IOException, InterruptedException {
		long timeoutLine = System.currentTimeMillis() - schemaFileExpiredTime;
		this.writeLock.lockInterruptibly();
		try {
			File[] fileInDir = baseDirFile.listFiles();
			for (File schemaFile : fileInDir) {
				if (schemaFile.isFile() && schemaFile.getName().endsWith(SCHEMA_FILE_SUFFIX)) {
					if (schemaFile.lastModified() < timeoutLine) {
						// Never use renameTo
						File bakSchemaFile = new File(schemaFile.getAbsolutePath() + CLEAN_MARKED_SCHEMA_FILE_SUFFIX);
						Files.move(schemaFile.toPath(), bakSchemaFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
					}
				}
			}
		} finally {
			this.writeLock.unlock();
		}
	}

	@Override
	public void clearCleanMarkedSchemaFile() throws InterruptedException {
		long timeoutLine = System.currentTimeMillis() - cleanMarkSchemaFileExpiredTime;
		this.writeLock.lockInterruptibly();
		try {
			File[] fileInDir = baseDirFile.listFiles();
			for (File schemaFile : fileInDir) {
				if (schemaFile.isFile() && schemaFile.getName().endsWith(CLEAN_MARKED_SCHEMA_FILE_SUFFIX)) {
					if (schemaFile.lastModified() < timeoutLine) {
						schemaFile.delete();
					}
				}
			}
		} finally {
			this.writeLock.unlock();
		}
	}

}
