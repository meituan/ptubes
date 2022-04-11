package com.meituan.ptubes.reader.schema.provider;

import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.SchemaParseException;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.common.utils.SetUtil;
import com.meituan.ptubes.reader.container.common.config.producer.RdsConfig;
import com.meituan.ptubes.reader.container.common.config.producer.RdsUserPassword;
import com.meituan.ptubes.reader.container.common.config.producer.TableConfig;
import com.meituan.ptubes.reader.container.common.config.service.ITableConfigChangeListener;
import com.meituan.ptubes.reader.container.common.config.service.ProducerConfigService;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.schema.common.SchemaVersion;
import com.meituan.ptubes.reader.schema.common.TableMeta;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;
import com.meituan.ptubes.reader.schema.service.ISchemaService;
import com.meituan.ptubes.reader.schema.util.SchemaGenerateUtil;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

@NotThreadSafe
public class ReadTaskSchemaProviderV2 extends AbstractLifeCycle implements ITableConfigChangeListener {

	private static final Logger LOG = LoggerFactory.getLogger(ReadTaskSchemaProviderV2.class);

	private final String name;
	private final String readerTaskName;
	private ISchemaService schemaService;

	private final ProducerConfigService producerConfigService;
	private volatile RdsConfig rdsConfig;
	private volatile RdsUserPassword rdsUserPassword;

	private Map<String, TreeMap<SchemaVersion, File>> versionBasedTableSchemas = new HashMap<>();
	private Map<String, TreeMap<Long, SchemaVersion>> timeBasedTableSchemas = new HashMap<>();
	private Map<String, VersionedSchema> schemaCache = new HashMap<>();

	public ReadTaskSchemaProviderV2(String readerTaskName, RdsConfig dbConfig, RdsUserPassword rdsUserPassword, ISchemaService schemaService, ProducerConfigService producerConfigService) {
		this.name = readerTaskName + "_ReadTaskSchemaProvider";
		this.readerTaskName = readerTaskName;
		this.schemaService = schemaService;
		this.rdsConfig = dbConfig;
		this.rdsUserPassword = rdsUserPassword;
		this.producerConfigService = producerConfigService;
	}

	public void resetDbInstanceConfig(RdsConfig newRdsConfig) {
		this.rdsConfig = newRdsConfig;
	}

	@Override
	public void onChange(Map<String, TableConfig> newConfig) throws IOException, InterruptedException, SchemaParseException {
		resetTableConfig(newConfig);
	}

	@Override
	public void doStart() {
		try {
			Set<String> tableSet = this.producerConfigService.getTableConfigs().keySet();
			reloadTableSchema(tableSet, true);

			this.producerConfigService.addTableConfigChangeListener(this.name, this);
		} catch (Exception e) {
			LOG.error("reader schema provider of reader task {} start error", readerTaskName, e);
			throw new PtubesRunTimeException("load schema for read task " + readerTaskName + " error", e);
		}
	}

	@Override
	public void doStop() {
		producerConfigService.rmTableConfigChangeListener(this.name);

		versionBasedTableSchemas.clear();
		timeBasedTableSchemas.clear();
        schemaCache.clear();
	}

	private void addTableSchema(String table, File file, VersionedSchema schema) {
		TreeMap<SchemaVersion, File> versionBasedSchemaFile = versionBasedTableSchemas.get(table);
		TreeMap<Long, SchemaVersion> timeBasedSchema = timeBasedTableSchemas.get(table);

		boolean vb = Objects.nonNull(versionBasedSchemaFile);
		boolean tb = Objects.nonNull(timeBasedSchema);

		if ((vb ^ tb) || ((vb && tb && versionBasedSchemaFile.size() != timeBasedSchema.size()))) {
			LOG.warn("{} table {} local schema meta is inconsistent", readerTaskName, table);
		}

        if (vb) {
            versionBasedSchemaFile.put(schema.getVersion(), file);
        } else {
            TreeMap<SchemaVersion, File> localVersionBasedSchemaFile = new TreeMap<>(new SchemaVersion.SchemaVersionComparator());
            localVersionBasedSchemaFile.put(schema.getVersion(), file);
            versionBasedTableSchemas.put(table, localVersionBasedSchemaFile);
        }
        if (tb) {
            timeBasedSchema.put(file.lastModified(), schema.getVersion());
        } else {
            TreeMap<Long, SchemaVersion> localTimeBasedSchema = new TreeMap<>(new FileModifiedTimeComparator());
            localTimeBasedSchema.put(file.lastModified(), schema.getVersion());
            timeBasedTableSchemas.put(table, localTimeBasedSchema);
        }
        schemaCache.put(table, schema);
	}
	private void unloadTableSchema(String table, boolean removeIfRedundant) {
		schemaCache.remove(table);

		// clear schema read view
		TreeMap<SchemaVersion, File> versionBasedSchemaFiles = versionBasedTableSchemas.remove(table);
		timeBasedTableSchemas.remove(table);
		if (removeIfRedundant && MapUtils.isNotEmpty(versionBasedSchemaFiles)) {
			for (File schemaFile : versionBasedSchemaFiles.values()) {
				schemaFile.delete();
			}
		}
	}
	private void unloadTableSchemas(Set<String> tableSet, boolean removeIfRedundant) {
		for (String table : tableSet) {
			unloadTableSchema(table, removeIfRedundant);
		}
	}
	private void reloadTableSchema(Set<String> tableSet, boolean removeIfRedundant) throws InterruptedException {
		Map<String, TreeMap<SchemaVersion, File>> localVersionBasedTableSchemas = schemaService.tableSortedVersionSchemaMap(tableSet, removeIfRedundant);
		Map<String, TreeMap<Long, SchemaVersion>> localTimeBasedTableSchemas = new HashMap<>();
		Map<String, VersionedSchema> localSchemaCache = new HashMap();
		for (Map.Entry<String, TreeMap<SchemaVersion, File>> entry : localVersionBasedTableSchemas.entrySet()) {
			String table = entry.getKey();
			TreeMap<SchemaVersion, File> versionBasedTableSchema = entry.getValue();

			TreeMap<Long, SchemaVersion> timeBasedTableSchema = new TreeMap<>(new FileModifiedTimeComparator());
			for (Map.Entry<SchemaVersion, File> schemaFileEntry : versionBasedTableSchema.entrySet()) {
				timeBasedTableSchema.put(schemaFileEntry.getValue().lastModified(), schemaFileEntry.getKey());
			}
			localTimeBasedTableSchemas.put(table, timeBasedTableSchema);

			Map.Entry<SchemaVersion, File> latestVersionSchema = versionBasedTableSchema.lastEntry();
			SchemaVersion latestVersion = latestVersionSchema.getKey();
			File latestVersionSchemaFile = latestVersionSchema.getValue();
			try {
				VersionedSchema schema = schemaService.parseSchemaFile(latestVersionSchemaFile);
				localSchemaCache.put(table, schema);
			} catch (Throwable te) {
				LOG.error("load schema {} into cache error", latestVersion, te);
			}
		}

        versionBasedTableSchemas.putAll(localVersionBasedTableSchemas);
        timeBasedTableSchemas.putAll(localTimeBasedTableSchemas);
        schemaCache.putAll(localSchemaCache);
	}

	public void resetTableConfig(Map<String, TableConfig> newTableConfigMap) throws InterruptedException, IOException {
		Set<String> outdatedTables = this.producerConfigService.getTableConfigs().keySet();
		Set<String> presentTables = newTableConfigMap.keySet();
		Pair<Set<String>, Set<String>> diffTables = SetUtil.minus(outdatedTables, presentTables);
		Set<String> staleTables = diffTables.getLeft();
		Set<String> freshTables = diffTables.getRight();
		// unload stale tables
		unloadTableSchemas(staleTables, true);
		// reload fresh tables, attention: please do not remove any schema files, keep removeIfRedundant false
		reloadTableSchema(freshTables, false);
	}

	public void resetTablePartKey(Map<String, TableConfig> newTableConfigMap) {
		throw new UnsupportedOperationException("unsupport change part key config");
	}

	/**
	 * generate schema by using "SELECT * FROM <table> where 0=1"
	 * @param table
	 * @return
	 * @throws PtubesException
	 */
	private int genNextTableSchemaVersion(String table) {
		TreeMap<SchemaVersion, File> versionBasedSchemas = versionBasedTableSchemas.get(table);
		if (MapUtils.isNotEmpty(versionBasedSchemas)) {
			return versionBasedSchemas.lastKey().getVersion() + 1;
		} else {
			return 1;
		}
	}

	/**
	 * read latest schema by comparing AccessTime, ModifiedTime or ChangeTime of files
	 * example:
	 * BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
	 */
	public VersionedSchema refreshTableSchema(String table, long produceTime) throws PtubesException {
		if (this.producerConfigService.allow(table)) {
			try {
				String[] tableInfos = table.split("\\.");
				if (tableInfos.length != 2) {
					LOG.error("refresh " + table + " schema error, tableName can not be parsed");
					throw new PtubesException("refresh " + table + " schema error, tableName can not be parsed");
				}
				String databaseName = tableInfos[0].trim();
				String tableName = tableInfos[1].trim();
				String partitionKey = this.producerConfigService.getPartitionKey(table);
				int nextVersion = genNextTableSchemaVersion(table);

				TableMeta tableMeta = SchemaGenerateUtil.genTableMeta(
					rdsConfig.getIp(),
					rdsConfig.getPort(),
					rdsUserPassword.getUserName(),
					rdsUserPassword.getPassword(),
					databaseName,
					tableName,
					partitionKey,
					String.valueOf(nextVersion));
				String rawSchema = JSONUtil.toJsonString(tableMeta, true);
				LOG.info("gen new table meta for {}.{}, meta: {}", databaseName, tableName, rawSchema);

				produceTime = (nextVersion == 1 ? 0 : produceTime);
				SchemaVersion schemaVersion = new SchemaVersion(table, nextVersion, produceTime);
				// write down disk
				File schemaFile = schemaService.saveTableSchema(schemaVersion, rawSchema);
				VersionedSchema versionedSchema = new VersionedSchema(rawSchema, tableMeta, schemaVersion);
				// update schema cache
                addTableSchema(table, schemaFile, versionedSchema);
				// return schema
				return versionedSchema;
			} catch (PtubesException be) {
				LOG.error("fail to generate schema from rds", be);
				throw be;
			} catch (IOException ioe) {
				LOG.error("schema persistence error", ioe);
				throw new PtubesException("schema persistence error", ioe);
			} catch (InterruptedException ie) {
				LOG.error("refresh " + table + " schema interrupted", ie);
				Thread.currentThread().interrupt(); // keep interruption flag
				throw new PtubesException("refresh " + table + " schema interrupted");
			} catch (Throwable te) {
				LOG.error("refresh " + table + " schema error", te);
				throw new PtubesException("refresh " + table + " schema error");
			}
		} else {
			/* never reach here */
			throw new PtubesException("no table config for " + table);
		}
	}

	public VersionedSchema loadTableSchemaFromCache(String table, long eventProduceTime) throws PtubesException, IOException, SchemaParseException {
		if (schemaCache.containsKey(table)) {
			VersionedSchema schema = schemaCache.get(table);
			if (schema.getVersion().getEventTime() <= eventProduceTime) {
				return schema;
			}
		}

		TreeMap<Long, SchemaVersion> localTimeBasedSchema = timeBasedTableSchemas.get(table);
		TreeMap<SchemaVersion, File> localVersionBasedSchema = versionBasedTableSchemas.get(table);
		if (MapUtils.isNotEmpty(localTimeBasedSchema) && MapUtils.isNotEmpty(localVersionBasedSchema)) {
			Map.Entry<Long, SchemaVersion> timeBasedSchemaEntry = localTimeBasedSchema.floorEntry(eventProduceTime);
			SchemaVersion schemaVersion = timeBasedSchemaEntry.getValue();
			if (Objects.nonNull(timeBasedSchemaEntry)) {
				File schemaFile = localVersionBasedSchema.get(schemaVersion);
				VersionedSchema schema = schemaService.parseSchemaFile(schemaFile);
				schemaCache.put(table, schema);
				return schema;
			} else {
				return refreshTableSchema(table, eventProduceTime);
			}
		} else {
			return refreshTableSchema(table, eventProduceTime);
		}
	}

	public static final class FileModifiedTimeComparator implements Comparator<Long> {
        @Override public int compare(Long o1, Long o2) {
            return o1.compareTo(o2);
        }
    }
}
