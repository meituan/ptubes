package com.meituan.ptubes.reader.producer.mysqlreplicator.utils;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.utils.IPUtil;
import com.meituan.ptubes.reader.container.common.constants.EncoderType;
import com.meituan.ptubes.reader.container.common.constants.ProducerConstants;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.PtubesFieldType;
import com.meituan.ptubes.reader.container.common.vo.KeyPair;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Pair;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.schema.common.FieldMeta;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;
import com.meituan.ptubes.reader.schema.util.SchemaPrimitiveTypes;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import com.meituan.ptubes.sdk.protocol.RdsPacket;
import org.apache.commons.lang3.StringUtils;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RecordUtil {
	private static final Logger LOG = LoggerFactory.getLogger(RecordUtil.class);

	private enum ExtendedValue {
		nanos,
		fmtDate,
		sourceIP,
		;
	}

	/**
	 * @param avroType
	 * @return
	 */
	private static int mysqlToJavaType(SchemaPrimitiveTypes avroType) {
		switch (avroType) {
			case BIT:
				return Types.BIT;
			case TINYINT:
			case TINYINT_UNSIGNED:
			case SMALLINT:
			case SMALLINT_UNSIGNED:
			case MEDIUMINT:
			case MEDIUMINT_UNSIGNED:
			case INTEGER:
			case INTEGER_UNSIGNED:
			case YEAR:
				return Types.INTEGER;
			case INT:
			case INT_UNSIGNED:
			case LONG:
			case BIGINT:
			case BIGINT_UNSIGNED:
				return Types.BIGINT;
			case FLOAT:
				return Types.FLOAT; // Types.REAL
			case DOUBLE:
				return Types.DOUBLE;
			case DECIMAL:
			case DECIMAL_UNSIGNED:
				return Types.DECIMAL;
			case CHAR:
				return Types.CHAR;
			case VARCHAR:
			case VARCHAR2:
			case NVARCHAR:
			case NVARCHAR2:
			case TEXT:
			case JSON:
				return Types.VARCHAR;
			case DATE:
				return Types.DATE;
			case TIME:
				return Types.TIME;
			case DATETIME:
			case TIMESTAMP:
				return Types.TIMESTAMP;
			case BINARY:
				return Types.BINARY;
			case BLOB:
			case TINYBLOB:
			case MEDIUMBLOB: // Types.BLOB;
			case VARBINARY:
			case RAW:
				return Types.VARBINARY;
			case LONGBLOB:
			case CLOB: // oracle
				return Types.LONGVARBINARY; // Types.CLOB;
			case GEOMETRY:
				return Types.VARCHAR;
			default:
				return Types.OTHER;
		}
	}

	/**
	 * reserved
	 */
	private static int mysqlTypeLength(SchemaPrimitiveTypes avroType) {
		return 0;
	}

	private static void setValue(FieldMeta fieldMeta, RdsPacket.Column.Builder columnBuilder, Object value) throws PtubesException {
		PtubesFieldType ptubesFieldType = fieldMeta.getPrimitiveType().getPtubesFieldType();

		switch (ptubesFieldType) {
			case DATE:
				org.apache.commons.lang3.tuple.Pair<Timestamp, String> dateValue = (org.apache.commons.lang3.tuple.Pair<Timestamp, String>) value;
				Timestamp tsDateValue = dateValue.getLeft();
				String stringDateValue = dateValue.getRight();

				columnBuilder.setValue(String.valueOf(tsDateValue.getTime()));
				if (StringUtils.isNotBlank(stringDateValue)) {
					columnBuilder.addProps(RdsPacket.Pair.newBuilder().setKey(ExtendedValue.fmtDate.name()).setValue(stringDateValue));
				}
				break;
			case TIMESTAMP:
			case DATETIME:
			case TIME:
				org.apache.commons.lang3.tuple.Pair<Timestamp, String> tsValue = (org.apache.commons.lang3.tuple.Pair<Timestamp, String>) value;
				Timestamp tsTimeValue = tsValue.getLeft();
				String stringTimeValue = tsValue.getRight();

				columnBuilder.setValue(String.valueOf(tsTimeValue.getTime()));
				columnBuilder.addProps(RdsPacket.Pair.newBuilder().setKey(ExtendedValue.nanos.name()).setValue(String.valueOf(tsTimeValue.getNanos())));
				if (StringUtils.isNotBlank(stringTimeValue)) {
					columnBuilder.addProps(RdsPacket.Pair.newBuilder().setKey(ExtendedValue.fmtDate.name()).setValue(stringTimeValue));
				}
				break;
			default:
				columnBuilder.setValue(String.valueOf(value));
				break;
		}
	}
	public static RdsPacket.RdsEvent generatePtubesEvent(
		String taskName,
		List<KeyPair> kps,
		BinlogInfo binlogInfo,
		VersionedSchema vs,
		EventType type,
		String dbTableName,
		Pair<Row> p,
		long timestampInBinlog,
		long replicatorInboundTS,
		String comment,
		String sql,
		long sourceIP
	) throws PtubesException {
		try {
			List<FieldMeta> orderedFieldMetas = vs.getTableMeta().getOrderedFieldMetaList();
			int colCount = p.getBefore() != null ? p.getBefore().getColumns().size() : p.getAfter().getColumns().size();
			if (orderedFieldMetas.size() != colCount) {
				List<String> colNames = orderedFieldMetas.stream().map(fieldMeta -> fieldMeta.getName()).collect(Collectors.toList());
				throw new PtubesException(
					"Mismatch in db schema vs avro schema schemaColCount:" + colNames.size() + " | schemaColList:"
						+ colNames + " | dbColSize:" + colCount);
			}

			if (ProducerConstants.HEARTBEAT_DB_TABLE_NAME.equals(dbTableName)) {
				type = EventType.HEARTBEAT;
			}

			List<Column> beforeColumns = (p.getBefore() != null ? p.getBefore().getColumns() : Collections.EMPTY_LIST);
			List<Column> afterColumns = (p.getAfter() != null ? p.getAfter().getColumns() : Collections.EMPTY_LIST);
			List<Column> defaultColumns = (p.getAfter() != null ? afterColumns : beforeColumns);

			// >>>>>>>>>>>>>>>>>>>>>>>>>  generate PB object  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
			RdsPacket.RdsEvent.Builder ptubesEventBuilder = RdsPacket.RdsEvent.newBuilder();

			String partitionKeyStr = vs.getTableMeta().getPkFieldListStr();
			String primaryKeysStr = vs.getTableMeta().getPrimaryKeyFieldListStr();
			List<String> primaryKeys = vs.getTableMeta().getPrimaryKeyFieldNameList(); //Collections.EMPTY_LIST;
			List<String> partitionKeys = vs.getTableMeta().getPkFieldNameList(); // Collections.EMPTY_LIST;

			RdsPacket.RdsHeader.Builder ptubesHeaderBuilder = RdsPacket.RdsHeader.newBuilder();
			if (binlogInfo instanceof MySQLBinlogInfo) {
				ptubesHeaderBuilder.setCheckpoint((RdsPacket.Checkpoint) binlogInfo.toCheckpoint());
			} else {
				throw new PtubesException("Unexpected binlog info class: " + binlogInfo.getClass());
			}
			ptubesHeaderBuilder.setEncode(EncoderType.PROTOCOL_BUFFER.name()).setExecuteTime(timestampInBinlog)
				.setTableName(dbTableName).setPartitionKey(partitionKeyStr).setPrimaryKey(primaryKeysStr).addProps(RdsPacket.Pair.newBuilder().setKey("sourceIP").setValue(IPUtil.longToIp(sourceIP, false)).build()).addProps(RdsPacket.Pair.newBuilder().setKey("replicatorInboundTS").setValue(String.valueOf(replicatorInboundTS)).build());

			RdsPacket.RowData.Builder rowDataBuilder;
			switch (type) {
				case UPDATE:
				case DELETE:
				case INSERT:
				case HEARTBEAT:
					rowDataBuilder = RdsPacket.RowData.newBuilder();
					for (int i = 0; i < orderedFieldMetas.size(); i++) {
						FieldMeta fieldMeta = orderedFieldMetas.get(i);

						String fieldName = fieldMeta.getName();
						String fieldType = fieldMeta.getDbType();
						SchemaPrimitiveTypes avroType = fieldMeta.getPrimitiveType();
						PtubesFieldType schemaType = fieldMeta.getPtubesFieldType();
						int javaSqlType = mysqlToJavaType(avroType);
						int sqlTypeLength = mysqlTypeLength(avroType);
						boolean isPartitionKey = partitionKeys.contains(fieldName);

						if (EventType.UPDATE.equals(type)) {
							RdsPacket.Column.Builder columnBeforeBuilder = RdsPacket.Column.newBuilder();
							RdsPacket.Column.Builder columnAfterBuilder = RdsPacket.Column.newBuilder();

							Object beforeRecordFieldObj = fetchFieldIntoRecord(beforeColumns.get(i), fieldMeta);
							beforeRecordFieldObj = beforeRecordFieldObj instanceof ByteBuffer ? new String(((ByteBuffer) beforeRecordFieldObj).array(), StandardCharsets.ISO_8859_1) : beforeRecordFieldObj;
							columnBeforeBuilder.setIndex(i).setSqlType(javaSqlType).setName(fieldName).setIsKey(primaryKeys.contains(fieldName)).setUpdated(false)
								.setLength(sqlTypeLength).setMysqlType(fieldType);
							if (Objects.isNull(beforeRecordFieldObj)) {
								columnBeforeBuilder.setIsNull(true);
							} else {
								columnBeforeBuilder.setIsNull(false);
								setValue(fieldMeta, columnBeforeBuilder, beforeRecordFieldObj);
							}

							Object afterRecordFieldObj = fetchFieldIntoRecord(afterColumns.get(i), fieldMeta);
							if (isPartitionKey) {
								kps.add(new KeyPair(afterRecordFieldObj, schemaType));
							}
							afterRecordFieldObj = afterRecordFieldObj instanceof ByteBuffer ? new String(((ByteBuffer) afterRecordFieldObj).array(), StandardCharsets.ISO_8859_1) : afterRecordFieldObj;
							columnAfterBuilder.setIndex(i).setSqlType(javaSqlType).setName(fieldName).setIsKey(primaryKeys.contains(fieldName)).setUpdated(false)
								.setLength(sqlTypeLength).setMysqlType(fieldType);
							if (Objects.isNull(afterRecordFieldObj)) {
								columnAfterBuilder.setIsNull(true);
							} else {
								columnAfterBuilder.setIsNull(false);
								setValue(fieldMeta, columnAfterBuilder, afterRecordFieldObj);
							}
							rowDataBuilder.putBeforeColumns(fieldName, columnBeforeBuilder.build())
								.putAfterColumns(fieldName, columnAfterBuilder.build());
						} else {
							RdsPacket.Column.Builder columnBuilder = RdsPacket.Column.newBuilder();
							Object defaultRecordFieldObj = fetchFieldIntoRecord(defaultColumns.get(i), fieldMeta);
							if (isPartitionKey) {
								kps.add(new KeyPair(defaultRecordFieldObj, schemaType));
							}
							defaultRecordFieldObj = defaultRecordFieldObj instanceof ByteBuffer ? new String(((ByteBuffer) defaultRecordFieldObj).array(), StandardCharsets.ISO_8859_1) : defaultRecordFieldObj;
							columnBuilder.setIndex(i).setSqlType(javaSqlType).setName(fieldName).setIsKey(primaryKeys.contains(fieldName)).setUpdated(false)
								.setLength(sqlTypeLength).setMysqlType(fieldType);
							if (Objects.isNull(defaultRecordFieldObj)) {
								columnBuilder.setIsNull(true);
							} else {
								columnBuilder.setIsNull(false);
								setValue(fieldMeta, columnBuilder, defaultRecordFieldObj);
							}

							if (EventType.DELETE.equals(type)) {
								rowDataBuilder.putBeforeColumns(fieldName, columnBuilder.build());
							} else {
								// insert or update
								rowDataBuilder.putAfterColumns(fieldName, columnBuilder.build());
							}
						}
					}

					ptubesEventBuilder.setHeader(ptubesHeaderBuilder.build()).setIsDdl(false).setRowData(rowDataBuilder.build()).setComment(comment).setSql(sql);
					switch (type) {
						case HEARTBEAT:
							ptubesEventBuilder.setEventType(RdsPacket.EventType.HEARTBEAT);
							break;
						case INSERT:
							ptubesEventBuilder.setEventType(RdsPacket.EventType.INSERT);
							break;
						case UPDATE:
							ptubesEventBuilder.setEventType(RdsPacket.EventType.UPDATE);
							break;
						case DELETE:
							ptubesEventBuilder.setEventType(RdsPacket.EventType.DELETE);
							break;
						default:
							throw new PtubesRunTimeException("unsupport event type: " + type.name());
					}
					break;
				case COMMIT:
					for (int i = 0; i < orderedFieldMetas.size(); i++) {
						FieldMeta fieldMeta = orderedFieldMetas.get(i);
						String fieldName = fieldMeta.getName();
						SchemaPrimitiveTypes avroType = fieldMeta.getPrimitiveType();
						if (partitionKeys.contains(fieldName)) {
							kps.add(new KeyPair(fetchFieldIntoRecord(afterColumns.get(i), fieldMeta), fieldMeta.getPtubesFieldType()));
						}
					}
					ptubesEventBuilder.setHeader(ptubesHeaderBuilder.build()).setIsDdl(false).setEventType(RdsPacket.EventType.TRANSACTIONEND);
					break;
				case DDL:
					String ddlSql = "";
					for (int i = 0; i < orderedFieldMetas.size(); i++) {
						FieldMeta fieldMeta = orderedFieldMetas.get(i);
						String fieldName = fieldMeta.getName();
						SchemaPrimitiveTypes avroType = fieldMeta.getPrimitiveType();
						if (partitionKeys.contains(fieldName)) {
							kps.add(new KeyPair(fetchFieldIntoRecord(afterColumns.get(i), fieldMeta), fieldMeta.getPtubesFieldType()));
						}
						if ("value".equals(fieldName)) {
							ddlSql = String.valueOf(fetchFieldIntoRecord(afterColumns.get(i), fieldMeta));
						}
					}
					ptubesEventBuilder.setHeader(ptubesHeaderBuilder.build()).setIsDdl(true).setEventType(RdsPacket.EventType.QUERY)
						.setDdlSchemaName(dbTableName).setSql(ddlSql);
					break;
				default:
					throw new PtubesRunTimeException("unsupport " + type.name() + " event yet");
			}

			return ptubesEventBuilder.build();
		} catch (Throwable te) {
			LOG.error("{} gen ptubes event from storage event cause error", taskName, te);
			throw new PtubesException(te);
		}
	}

	private static Object fetchFieldIntoRecord(Column origin, FieldMeta fieldMeta) throws PtubesException {
		boolean isNullValue = Objects.isNull(origin);
		if (isNullValue) {
			return null;
		} else {
			return Replicator2AvroConvert.convert(origin, fieldMeta);
		}
	}
}
