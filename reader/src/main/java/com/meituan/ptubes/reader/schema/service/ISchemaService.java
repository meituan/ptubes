package com.meituan.ptubes.reader.schema.service;

import com.meituan.ptubes.common.exception.SchemaNotFoundException;
import com.meituan.ptubes.common.exception.SchemaParseException;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import com.meituan.ptubes.reader.schema.common.SchemaVersion;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;

/**
 * Read and write schema services
 */
public interface ISchemaService {

	// ============================= load meta data ==================================
	VersionedSchema parseSchemaFile(File schemaFile) throws IOException, SchemaParseException;

	/**
	 * reload all schema files of a table
	 *
	 * @param table
	 * @return
	 * @throws InterruptedException
	 */
	TreeMap<SchemaVersion, File> tableSortedVersionSchemaMap(String table) throws InterruptedException;

	/**
	 * reload all schema files of a set of tables
	 *
	 * @param tables
	 * @param removeIfRequire
	 * @return
	 * @throws InterruptedException
	 */
	Map<String, TreeMap<SchemaVersion, File>> tableSortedVersionSchemaMap(Set<String> tables, boolean removeIfRequire) throws InterruptedException;

	/**
	 * load latest version of a set of tables
	 *
	 * @param tables
	 * @return
	 * @throws InterruptedException
	 */
	Map<String, Integer> tableVersion(Set<String> tables) throws InterruptedException;

	// ============================= load schema ==================================

	/**
	 * load latest schema of a set of tables
	 *
	 * @param tables
	 * @return
	 */
	Map<String, VersionedSchema> fetchLatestTableSchema(Set<String> tables) throws IOException, SchemaParseException, InterruptedException;

	/**
	 * load latest schema of a set of tables
	 *
	 * @param tables
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	Map<String, TreeMap<SchemaVersion, VersionedSchema>> fetchAllTableSchemas(Set<String> tables)
		throws IOException, SchemaParseException, InterruptedException;

	/**
	 * load latest schema of a set of tables by comparing file in modification time
	 *
	 * @param tables
	 * @param time
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	Map<String, VersionedSchema> fetchTableSchemaByTime(Set<String> tables, long time)
		throws IOException, SchemaParseException, InterruptedException;

	/**
	 *
	 * @param tables
	 * @param maxCacheNum
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	Map<String, TreeMap<SchemaVersion, VersionedSchema>> fetchPartialTableSchemas(Set<String> tables,
		int maxCacheNum) throws IOException, SchemaParseException, InterruptedException;

	/**
	 * load latest schema of a set of tables by comparing file in version
	 *
	 * @param tableFullName
	 * @return
	 */
	VersionedSchema loadLatestTableSchema(String tableFullName) throws InterruptedException, SchemaNotFoundException, SchemaParseException;

	/**
	 * load schema of a table by time
	 *
	 * @param tableFullName
	 * @param timestamp
	 * @return
	 */
	VersionedSchema loadTableSchemaByTime(String tableFullName, long timestamp) throws InterruptedException, SchemaNotFoundException, SchemaParseException;

	/**
	 * load schema of a table by version
	 *
	 * @param schemaVersion
	 * @return
	 */
	VersionedSchema loadTableSchemaByVersion(SchemaVersion schemaVersion) throws InterruptedException, SchemaNotFoundException, SchemaParseException;

	// ============================= save ==================================

	/**
	 * save schema for a table
	 *
	 * @param schemaVersion
	 * @param schema
	 * @return
	 */
	File saveTableSchema(SchemaVersion schemaVersion, String schema) throws IOException, InterruptedException;

	// ================================== cleaner ====================================
	/**
	 * mark expired schema files
	 *
	 * @return
	 */
	@Deprecated
	void markCleanFlagOnSchemaFile() throws IOException, InterruptedException;

	/**
	 * clear schema files marked delete flag
	 *
	 * @return
	 */
	@Deprecated
	void clearCleanMarkedSchemaFile() throws InterruptedException;
}
