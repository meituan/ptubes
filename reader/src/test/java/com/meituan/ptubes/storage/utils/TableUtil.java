package com.meituan.ptubes.storage.utils;

import com.meituan.ptubes.common.exception.SchemaParseException;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.Gtid;
import com.meituan.ptubes.reader.container.common.vo.KeyPair;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.schema.common.FieldMeta;
import com.meituan.ptubes.reader.schema.common.SchemaVersion;
import com.meituan.ptubes.reader.schema.common.TableMeta;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;
import com.meituan.ptubes.reader.schema.util.SchemaGenerateUtil;
import com.meituan.ptubes.reader.schema.util.SchemaHelper.SchemaType;
import com.meituan.ptubes.sdk.protocol.RdsPacket;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;


public class TableUtil {
	public final static String PATH_ROOT = TableUtil.class.getResource("/").getFile();
	public final static String TEST01_TABLE_NAME = "test.test01";
	public final static String TEST02_TABLE_NAME = "test.test02";

	public static VersionedSchema getSchema(String dbTableName) throws IOException, SchemaParseException {
		File file = new File(PATH_ROOT + dbTableName + ".1.dtsc");
		String rawSchema = IOUtils.toString(new FileInputStream(file));
		TableMeta tableMeta = SchemaGenerateUtil.parseRawSchema(rawSchema);
		SchemaVersion schemaVersion = new SchemaVersion(dbTableName, 1);
		return new VersionedSchema(rawSchema, tableMeta, schemaVersion);
	}

	public static GenericRecord genInsertRecord(String tableName) throws Exception {
		VersionedSchema vs = getSchema(tableName);
		return genGenericRecord(vs, null, genRow(), SchemaType.insert);
	}

	public static GenericRecord genUpdateRecord(String tableName) throws Exception {
		VersionedSchema vs = getSchema(tableName);
		return genGenericRecord(vs, genRow(), genRow(), SchemaType.update);
	}

	public static GenericRecord genDeleteRecord(String tableName) throws Exception {
		VersionedSchema vs = getSchema(tableName);
		return genGenericRecord(vs, genRow(), null, SchemaType.delete);
	}

	public static RdsPacket.RdsEvent genInsertPtubesRecord(VersionedSchema vs, List<KeyPair> kps, BinlogInfo binlogInfo) {
		return genPtubesRecord(vs, kps, binlogInfo, null, genRow(), SchemaType.insert);
	}

	public static RdsPacket.RdsEvent genUpdatePtubesRecord(VersionedSchema vs, List<KeyPair> kps, BinlogInfo binlogInfo) {
		return genPtubesRecord(vs, kps, binlogInfo, genRow(), genRow(), SchemaType.update);
	}

	public static RdsPacket.RdsEvent genDeletePtubesRecord(VersionedSchema vs, List<KeyPair> kps, BinlogInfo binlogInfo) {
		return genPtubesRecord(vs, kps, binlogInfo, genRow(), null, SchemaType.delete);
	}

	private static RdsPacket.RdsEvent genPtubesRecord(
		VersionedSchema vs,
		List<KeyPair> kps,
		BinlogInfo binlogInfo,
		Map<String, Object> preAvroFieldCol,
		Map<String, Object> afterAvroFieldCol,
		SchemaType type
	) {
		TableMeta tableMeta = vs.getTableMeta();
		List<FieldMeta> fieldMetas = tableMeta.getOrderedFieldMetaList();

		Map<String, Object> nonNullAvroFieldCol = MapUtils.isNotEmpty(afterAvroFieldCol) ? afterAvroFieldCol : preAvroFieldCol;
		String fullTableName = tableMeta.getTableOwner() + "." + tableMeta.getTable();
		RdsPacket.RdsEvent.Builder ptubesEventBuilder = RdsPacket.RdsEvent.newBuilder();
		RdsPacket.RdsHeader.Builder ptubesHeaderBuilder = RdsPacket.RdsHeader.newBuilder();

		RdsPacket.RowData.Builder rowDataBuilder = RdsPacket.RowData.newBuilder();

		if (SourceType.MySQL.equals(binlogInfo.getSourceType())) {
			RdsPacket.Checkpoint.Builder checkpointBuilder = RdsPacket.Checkpoint.newBuilder();
			MySQLBinlogInfo mySQLBinlogInfo = (MySQLBinlogInfo) binlogInfo;
			checkpointBuilder.setUuid(Gtid.getUuidStr(mySQLBinlogInfo.getUuid())).setTransactionId(mySQLBinlogInfo.getTxnId()).setEventIndex(mySQLBinlogInfo.getEventIndex())
				.setServerId((int) mySQLBinlogInfo.getServerId()).setBinlogFile(mySQLBinlogInfo.getBinlogId()).setBinlogOffset(mySQLBinlogInfo.getBinlogOffset())
				.setTimestamp(mySQLBinlogInfo.getTimestamp());
			ptubesHeaderBuilder.setCheckpoint(checkpointBuilder.build());
		} else {
			throw new UnsupportedOperationException("Unsupport sourceType: " + binlogInfo.getSourceType());
		}

		ptubesHeaderBuilder.setEncode("PROTOCOL_BUFFER").setExecuteTime(System.currentTimeMillis())
			.setTableName(fullTableName).setPartitionKey(tableMeta.getPkFieldListStr()).setPrimaryKey(tableMeta.getPrimaryKeyFieldListStr())
			.addProps(RdsPacket.Pair.newBuilder().setKey("sourceIP").setValue("127.0.0.1").build());
		for (FieldMeta fieldMeta : fieldMetas) {
			if (fieldMeta.isPartitionKey()) {
				kps.add(new KeyPair(nonNullAvroFieldCol.get(fieldMeta.getName()), fieldMeta.getPtubesFieldType()));
			}
			if (MapUtils.isNotEmpty(preAvroFieldCol)) {
				RdsPacket.Column preColumn = genColumn(fieldMeta, preAvroFieldCol.get(fieldMeta.getName()));
				rowDataBuilder.putBeforeColumns(fieldMeta.getName(), preColumn);
			}
			if (MapUtils.isNotEmpty(afterAvroFieldCol)) {
				RdsPacket.Column preColumn = genColumn(fieldMeta, afterAvroFieldCol.get(fieldMeta.getName()));
				rowDataBuilder.putAfterColumns(fieldMeta.getName(), preColumn);
			}
		}

		ptubesEventBuilder.setHeader(ptubesHeaderBuilder.build()).setIsDdl(false).setRowData(rowDataBuilder.build()).setComment("").setEventType(toEventType(type));
		return ptubesEventBuilder.build();
	}

	private static RdsPacket.EventType toEventType(SchemaType type) {
		switch (type) {
			case ddl:
				return RdsPacket.EventType.QUERY;
			case insert:
				return RdsPacket.EventType.INSERT;
			case update:
			case upsert:
				return RdsPacket.EventType.UPDATE;
			case delete:
				return RdsPacket.EventType.DELETE;
			default:
				return RdsPacket.EventType.TRANSACTIONEND;
		}
	}

	private static RdsPacket.Column genColumn(FieldMeta fieldMeta, Object fieldValue) {
		RdsPacket.Column.Builder columnBuilder = RdsPacket.Column.newBuilder();
		int pos = fieldMeta.getIndex();
		boolean isNullValue = Objects.isNull(fieldValue);
		columnBuilder = RdsPacket.Column.newBuilder();
		columnBuilder.setIndex(pos).setName(fieldMeta.getName()).setIsKey(fieldMeta.isPrimaryKey()).setUpdated(false)
			.setMysqlType(fieldMeta.getDbType()).setLength(0).setSqlType(Types.OTHER);
		if (isNullValue) {
			columnBuilder.setIsNull(true);
		} else {
			columnBuilder.setIsNull(false).setValue(toStringValue(fieldValue));
		}
		return columnBuilder.build();
	}

	private static String toStringValue(Object value) {
		if (value instanceof byte[]) {
			return new String((byte[])value, StandardCharsets.ISO_8859_1);
		} else if (value instanceof ByteBuffer) {
			ByteBuffer buffer = (ByteBuffer) value;
			if (buffer.hasArray()) {
				return new String(buffer.array(), StandardCharsets.ISO_8859_1);
			} else {
				byte[] byteArray = new byte[buffer.remaining()];
				buffer.get(byteArray, 0, buffer.remaining());
				return new String(byteArray, StandardCharsets.ISO_8859_1);
			}
		} else {
			return value.toString();
		}
	}

	public static GenericRecord genGenericRecord(VersionedSchema vs, Map<String, Object> preAvroFieldCol,
			Map<String, Object> afterAvroFieldCol, SchemaType type) throws Exception {
		return null;
	}

	public static Map<String, Object> genRow() {
		Map<String, Object> columns = new HashMap<>();
		columns.put("id", ColumnRandomUtil.getLongLongColumn());
		columns.put("year", ColumnRandomUtil.getYearColumn());
		columns.put("bigint", ColumnRandomUtil.getLongLongColumn());
		columns.put("double", ColumnRandomUtil.getDoubleColumn());
		columns.put("float", ColumnRandomUtil.getFloatColumn());
		columns.put("mediumint", ColumnRandomUtil.getInt24Column());
		columns.put("real", ColumnRandomUtil.getDoubleColumn());
		columns.put("smallint", ColumnRandomUtil.getShortColumn());
		columns.put("tinyint", ColumnRandomUtil.getTinyColumn());
		columns.put("char", ColumnRandomUtil.getStringColumn());
		columns.put("longtext", ColumnRandomUtil.getStringColumn());
		columns.put("mediumtext", ColumnRandomUtil.getStringColumn());
		columns.put("text", ColumnRandomUtil.getStringColumn());
		columns.put("tinytext", ColumnRandomUtil.getStringColumn());
		columns.put("json_col", ColumnRandomUtil.getJSONColumn());
		columns.put("utime", ColumnRandomUtil.getTimestamp2Column());
		columns.put("ctime", ColumnRandomUtil.getTimestamp2Column());
		columns.put("set_id_update", ColumnRandomUtil.getLongLongColumn());

		return columns;
	}

	private static void fillDiffMap(Map<String, Object> map, String name, Object obj) {
		if (obj instanceof ByteBuffer) {
			obj = byteArr2HexStr(((ByteBuffer) obj).array());
		}
		map.put(name, obj);
	}

	public static String byteArr2HexStr(byte[] byteArr) {
		StringBuilder sb = new StringBuilder();
		for (byte b : byteArr) {
			String str = Integer.toHexString(b & 0xFF);
			if (str.length() < 2) {
				sb.append("0");
			}
			sb.append(str);
		}
		return sb.toString();
	}

}
