package com.meituan.ptubes.reader.schema.util;

import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.SchemaParseException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.common.utils.StringUtil;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Transport;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.Query;
import com.meituan.ptubes.reader.schema.common.ColumnMetaData;
import com.meituan.ptubes.reader.schema.common.FieldMeta;
import com.meituan.ptubes.reader.schema.common.IndexMetaData;
import com.meituan.ptubes.reader.schema.common.TableMeta;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections.CollectionUtils;

public class SchemaGenerateUtil {

	public static final Logger LOG = LoggerFactory.getLogger(SchemaGenerateUtil.class);

	public static TableMeta genTableMeta(String host, int port, String user, String password, String database, String table,
		String partitionKeysString, String version)  throws Exception {
		try {
			String metaVersion = database + "." + table + ".V" + version;
			List<FieldMeta> fieldMetas = new ArrayList<>();

			Transport transport = Query.getDefaultTransport(host, port, user, password);
			IndexMetaData priIndexMetaData = null;
			List<ColumnMetaData> orderedColumnMetaData = Collections.EMPTY_LIST;
			try {
				Query q = new Query(transport);
				priIndexMetaData = q.getPrimaryIndexMetaData(database, table);
				orderedColumnMetaData = q.getOrderedColumnMetaData(database, table);
			} finally {
				transport.disconnect();
			}

			List<String> partitionKeys = StringUtil.stringListBreak(partitionKeysString, ",");
			List<String> primaryKeys = priIndexMetaData.getOrderedCols();
			String primaryKeyString = StringUtil.stringListConjunction(primaryKeys, ",");
			boolean treatPriKeyAsPartKey = !CollectionUtils.isNotEmpty(partitionKeys);
			partitionKeys = (treatPriKeyAsPartKey ? primaryKeys : partitionKeys);
			partitionKeysString = (treatPriKeyAsPartKey ? primaryKeyString : partitionKeysString);

			List<FieldMeta> fieldMetaList = new ArrayList<>();
			for (ColumnMetaData columnMetaData : orderedColumnMetaData) {
				SchemaPrimitiveTypes schemaPrimitiveTypes = convertSchemaPrimitiveType(columnMetaData);
				FieldMeta fieldMeta = new FieldMeta(
					columnMetaData.getPos() - 1,
					columnMetaData.getName(),
					columnMetaData.getDataType().toUpperCase(),
					schemaPrimitiveTypes,
					columnMetaData.getDefValue(),
					columnMetaData.isUnsigned(),
					columnMetaData.isNullable(),
					primaryKeys.contains(columnMetaData.getName()),
					partitionKeys.contains(columnMetaData.getName()),
					columnMetaData.getNumbericPrecision(),
					columnMetaData.getNumbericScale(),
					columnMetaData.getDatetimePrecision()
				);
				fieldMetaList.add(fieldMeta);
			}

			return new TableMeta(metaVersion, database, table, fieldMetaList, partitionKeysString, primaryKeyString);
		} catch (Throwable te) {
			LOG.error("get {}.{} table meta error", database, table, te);
			throw new PtubesException(te);
		}
	}

	public static TableMeta parseRawSchema(String rawSchema) throws SchemaParseException {
		try {
			Optional<TableMeta> tableMeta = JSONUtil.jsonToSimpleBean(rawSchema, TableMeta.class);
			if (tableMeta.isPresent()) {
				return tableMeta.get();
			} else {
				throw new SchemaParseException("invalid schema: " + rawSchema);
			}
		} catch (Throwable te) {
			LOG.error("parse schema error, schema content:\n{}", rawSchema, te);
			throw new SchemaParseException(te);
		}
	}

	private static SchemaPrimitiveTypes convertSchemaPrimitiveType(ColumnMetaData columnMetaData) {
		String dbType = columnMetaData.getDataType().toUpperCase();
		if (SchemaPrimitiveTypes.isSignSensitive(dbType)) {
			dbType = columnMetaData.isUnsigned() ? dbType + "_UNSIGNED" : dbType;
		}
		return SchemaPrimitiveTypes.valueOf(dbType);
	}

}
