package com.meituan.ptubes.reader.schema.provider;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.SchemaNotFoundException;
import com.meituan.ptubes.common.exception.SchemaParseException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.SetUtil;
import com.meituan.ptubes.reader.container.common.config.producer.RdsConfig;
import com.meituan.ptubes.reader.container.common.config.producer.RdsUserPassword;
import com.meituan.ptubes.reader.container.common.config.producer.TableConfig;
import com.meituan.ptubes.reader.container.common.config.service.ITableConfigChangeListener;
import com.meituan.ptubes.reader.container.common.config.service.ProducerConfigService;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.schema.common.SchemaVersion;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;
import com.meituan.ptubes.reader.schema.common.VirtualSchema;
import com.meituan.ptubes.reader.schema.service.ISchemaService;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;

public class ReadTaskSchemaProvider extends AbstractLifeCycle implements ITableConfigChangeListener {

	private static final Logger LOG = LoggerFactory.getLogger(ReadTaskSchemaProvider.class);

	private final String name;
	private final String readTaskName;
	private ISchemaService schemaService;
	private TableSchemaVersionViewer schemaVersionViewer;
	private Map<String, VersionedSchema> schemaCache = new ConcurrentHashMap<>();
	private final ProducerConfigService producerConfigService;
	private volatile RdsConfig rdsConfig;
	private RdsUserPassword rdsUserPassword;

	public ReadTaskSchemaProvider(String readTaskName, RdsConfig dbConfig, RdsUserPassword rdsUserPassword, ISchemaService schemaService, ProducerConfigService producerConfigService) {
		this.name = readTaskName + "_ReadTaskSchemaProvider";
		this.readTaskName = readTaskName;
		this.schemaService = schemaService;
		this.rdsConfig = dbConfig;
		this.rdsUserPassword = rdsUserPassword;
		this.producerConfigService = producerConfigService;
	}

	@Override
	public void doStart() {
		try {
			this.schemaCache.putAll(schemaService.fetchLatestTableSchema(this.producerConfigService.getTableConfigs().keySet()));
			Map<String, Integer> schemaMaxVersion = new HashMap<>();
			for (Map.Entry<String, VersionedSchema> entry : this.schemaCache.entrySet()) {
				schemaMaxVersion.put(entry.getKey(), entry.getValue().getVersion().getVersion());
			}
			this.schemaVersionViewer = new TableSchemaVersionViewer(schemaMaxVersion);

			schemaCache.put(VirtualSchema.INTERNAL_TABLE_SCHEMA.getVersion().getTableFullName(),
				VirtualSchema.INTERNAL_TABLE_SCHEMA);

			this.producerConfigService.addTableConfigChangeListener(this.name, this);
		} catch (Exception e) {
			LOG.error("reader schema provider of reader task {} start error", readTaskName, e);
			throw new PtubesRunTimeException("load schema for read task " + readTaskName + " error", e);
		}
	}

	@Override
	public void doStop() {
		this.producerConfigService.rmTableConfigChangeListener(this.name);
		this.schemaCache.clear();
	}

	public void resetTableConfig(Map<String, TableConfig> newTableConfigMap) throws InterruptedException, IOException,
        SchemaParseException {
		// The schema is reserved, the client needs to use
		Set<String> outdatedTables = this.producerConfigService.getTableConfigs().keySet();
		Set<String> presentTables = newTableConfigMap.keySet();
		Pair<Set<String>, Set<String>> diffTables = SetUtil.minus(outdatedTables, presentTables);
		Set<String> staleTables = diffTables.getLeft();
		Set<String> freshTables = diffTables.getRight();
		// stale tables
		schemaVersionViewer.removeTableVersion(staleTables);
		for (String table : staleTables) {
			schemaCache.remove(table);
		}
		// fresh tables
		Map<String, VersionedSchema> freshSchemaVersions = schemaService.fetchLatestTableSchema(freshTables);
		for (Map.Entry<String, VersionedSchema> freshSchemaVersionEntry : freshSchemaVersions.entrySet()) {
			schemaVersionViewer.refreshTableVersion(freshSchemaVersionEntry.getKey(), freshSchemaVersionEntry.getValue().getVersion().getVersion());
		}
	}

	/**
	 * Roaming to find the schema
	 * @param schemaVersion
	 * @param distance
	 * @return
	 * @throws InterruptedException
	 * @throws SchemaNotFoundException
	 */
	public VersionedSchema loadTableSchema(SchemaVersion schemaVersion, int distance) throws InterruptedException, SchemaNotFoundException, SchemaParseException {
		String table = schemaVersion.getTableFullName();
		int targetVersion = schemaVersion.getVersion() + distance;
		if (targetVersion <= 0 || schemaVersionViewer.reachUpperVersionLimit(schemaVersion.getTableFullName(), targetVersion)) {
			throw new SchemaNotFoundException("table:" + table + " version:" + targetVersion + " reach upper/lower limit");
		}

		VersionedSchema versionedSchema = schemaService.loadTableSchemaByVersion(new SchemaVersion(table, targetVersion));
		schemaCache.put(table, versionedSchema);
		return versionedSchema;
	}

	@Override
	public void onChange(Map<String, TableConfig> newConfig) throws IOException, InterruptedException, SchemaParseException {
		resetTableConfig(newConfig);
	}

	private static class TableSchemaVersionViewer {
		// todo: use Map<String, AtomicInteger> instead
		private final Map<String, Integer> tableVersionView = new ConcurrentHashMap<>();

		public TableSchemaVersionViewer(Map<String, Integer> tableVersionView) {
			if (MapUtils.isEmpty(tableVersionView) == false) {
				this.tableVersionView.putAll(tableVersionView);
			}
		}

		public int nextTableVersion(String table) {
			if (tableVersionView.containsKey(table)) {
				return tableVersionView.get(table) + 1;
			} else {
				return 1;
			}
		}

		public void refreshTableVersion(String table, int version) {
			tableVersionView.put(table, version);
		}

		public boolean reachUpperVersionLimit(String table, int version) {
			if (tableVersionView.containsKey(table)) {
				return version > tableVersionView.get(table);
			} else {
				return true;
			}
		}

		public void removeTableVersion(Set<String> tables) {
			for (String table : tables) {
				tableVersionView.remove(table);
			}
		}
	}
}
