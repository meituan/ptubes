package com.meituan.ptubes.reader.schema.common;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.SchemaParseException;
import com.meituan.ptubes.reader.schema.util.SchemaGenerateUtil;

/**
 * {
 * 		id:	int(always 0),
 * 		event_type:	string(see @link EventType)
 * 		value: string
 * 	}
 */

/**
 * {
 *   "tag" : "1.0",
 *   "metaVersion" : "internal_database.internal_table.V1",
 *   "tableOwner" : "internal_database",
 *   "table" : "internal_table",
 *   "pkFieldListStr" : "id",
 *   "primaryKeyFieldListStr" : "id",
 *   "orderedFieldMetaList" : [ {
 *     "index" : 0,
 *     "name" : "id",
 *     "dbType" : "INT",
 *     "defaultValue" : null,
 *     "hasDefaultValue" : false,
 *     "precision" : 100,
 *     "scale" : 0,
 *     "partitionKey" : true,
 *     "nullable" : false,
 *     "primaryKey" : true,
 *     "unsigned" : false
 *   }, {
 *     "index" : 1,
 *     "name" : "event_type",
 *     "dbType" : "INT",
 *     "defaultValue" : null,
 *     "hasDefaultValue" : false,
 *     "precision" : 100,
 *     "scale" : 0,
 *     "partitionKey" : false,
 *     "nullable" : false,
 *     "primaryKey" : false,
 *     "unsigned" : false
 *   }, {
 *     "index" : 2,
 *     "name" : "value",
 *     "dbType" : "VARCHAR",
 *     "defaultValue" : null,
 *     "hasDefaultValue" : false,
 *     "precision" : 256,
 *     "scale" : 0,
 *     "partitionKey" : false,
 *     "nullable" : false,
 *     "primaryKey" : false,
 *     "unsigned" : false
 *   }]
 * }
 */
public class VirtualSchema {

	private static final VirtualSchema INTERNAL_TABLE;
	public static final VersionedSchema INTERNAL_TABLE_SCHEMA;
	static {
		try {
			INTERNAL_TABLE = new VirtualSchema(
		"internal_database.internal_table",
				"{\n" +
					"  \"tag\" : \"1.0\",\n" +
					"  \"metaVersion\" : \"internal_database.internal_table.V1\",\n" +
					"  \"tableOwner\" : \"internal_database\",\n" +
					"  \"table\" : \"internal_table\",\n" +
					"  \"pkFieldListStr\" : \"id\",\n" +
					"  \"primaryKeyFieldListStr\" : \"id\",\n" +
					"  \"orderedFieldMetaList\" : [ {\n" +
					"    \"index\" : 0,\n" +
					"    \"name\" : \"id\",\n" +
					"    \"dbType\" : \"INT\",\n" +
					"    \"defaultValue\" : null,\n" +
					"    \"hasDefaultValue\" : false,\n" +
					"    \"precision\" : 100,\n" +
					"    \"scale\" : 0,\n" +
					"    \"primitiveType\" : \"INT\",\n" +
					"    \"partitionKey\" : true,\n" +
					"    \"nullable\" : false,\n" +
					"    \"primaryKey\" : true,\n" +
					"    \"unsigned\" : false\n" +
					"  }, {\n" +
					"    \"index\" : 1,\n" +
					"    \"name\" : \"event_type\",\n" +
					"    \"dbType\" : \"INT\",\n" +
					"    \"defaultValue\" : null,\n" +
					"    \"hasDefaultValue\" : false,\n" +
					"    \"precision\" : 100,\n" +
					"    \"scale\" : 0,\n" +
					"    \"primitiveType\" : \"INT\",\n" +
					"    \"partitionKey\" : false,\n" +
					"    \"nullable\" : false,\n" +
					"    \"primaryKey\" : false,\n" +
					"    \"unsigned\" : false\n" +
					"  }, {\n" +
					"    \"index\" : 2,\n" +
					"    \"name\" : \"value\",\n" +
					"    \"dbType\" : \"VARCHAR\",\n" +
					"    \"defaultValue\" : null,\n" +
					"    \"hasDefaultValue\" : false,\n" +
					"    \"precision\" : 256,\n" +
					"    \"scale\" : 0,\n" +
					"    \"primitiveType\" : \"VARCHAR\",\n" +
					"    \"partitionKey\" : false,\n" +
					"    \"nullable\" : false,\n" +
					"    \"primaryKey\" : false,\n" +
					"    \"unsigned\" : false\n"
					+ "  }]\n"
					+ "}"
//		"{\n"
//		   + "  \"meta\" : \"dbFieldName=internal_database.internal_table;dbFieldType=internal_table;partkey=id;prikey=id\",\n"
//		   + "  \"name\" : \"internal_table_V1\",\n"
//		   + "  \"namespace\" : \"internal_database\",\n"
//		   + "  \"doc\" : \"Auto-generated Avro schema for internal_database.internal_table. Generated at January 1, 2019 00:00:00 am CST\",\n"
//		   + "  \"type\" : \"record\",\n"
//		   + "  \"fields\" : [ {\n"
//		   + "    \"default\" : null,\n"
//		   + "    \"meta\" : \"dbFieldName=id;dbFieldPosition=0;dbFieldType=INT;\",\n"
//		   + "    \"name\" : \"id\",\n"
//		   + "    \"type\" : [ \"null\", \"long\" ]\n"
//		   + "  }, {\n"
//		   + "    \"default\" : null,\n"
//		   + "    \"meta\" : \"dbFieldName=event_type;dbFieldPosition=1;dbFieldType=INT;\",\n"
//		   + "    \"name\" : \"event_type\",\n"
//		   + "    \"type\" : [ \"null\", \"long\" ]\n"
//		   + "  }, {\n"
//		   + "    \"default\" : null,\n"
//		   + "    \"meta\" : \"dbFieldName=value;dbFieldPosition=2;dbFieldType=VARCHAR;\",\n"
//		   + "    \"name\" : \"value\",\n"
//		   + "    \"type\" : [ \"null\", \"string\" ]\n"
//		   + "  } ]\n" + "}"
		   	);
			INTERNAL_TABLE_SCHEMA = new VersionedSchema(INTERNAL_TABLE.getInternalRawSchema(), INTERNAL_TABLE.getTableMeta(), INTERNAL_TABLE.getSchemaVersion());
		} catch (Throwable te) {
			throw new PtubesRunTimeException("generate internal table schema error", te);
		}
	}

	private String dbTableName;
	private String internalRawSchema;
	private TableMeta tableMeta;
	private SchemaVersion schemaVersion;

	public VirtualSchema(String dbTableName, String internalRawSchema) throws SchemaParseException {
		this.dbTableName = dbTableName;
		this.internalRawSchema = internalRawSchema;
		this.tableMeta = SchemaGenerateUtil.parseRawSchema(internalRawSchema);
		this.schemaVersion = new SchemaVersion(dbTableName, 1);
	}

	public String getDbTableName() {
		return dbTableName;
	}

	public void setDbTableName(String dbTableName) {
		this.dbTableName = dbTableName;
	}

	public String getInternalRawSchema() {
		return internalRawSchema;
	}

	public void setInternalRawSchema(String internalRawSchema) {
		this.internalRawSchema = internalRawSchema;
	}

	public TableMeta getTableMeta() {
		return tableMeta;
	}

	public void setTableMeta(TableMeta tableMeta) {
		this.tableMeta = tableMeta;
	}

	public SchemaVersion getSchemaVersion() {
		return schemaVersion;
	}

	public void setSchemaVersion(SchemaVersion schemaVersion) {
		this.schemaVersion = schemaVersion;
	}
}
