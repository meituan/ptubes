package com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl;

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.util.Collections;
import java.util.Objects;
import java.util.TreeMap;
import com.meituan.ptubes.reader.container.common.vo.GtidSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.error.TransportException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.ColumnDefPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.EOFPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.ErrorPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.ResultSetHeaderPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.ResultSetRowPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.command.ComQuery;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Packet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Transport;
import com.meituan.ptubes.reader.schema.common.ColumnMetaData;
import com.meituan.ptubes.reader.schema.common.IndexMetaData;
import com.meituan.ptubes.reader.schema.common.ResultSetHeader;
import com.meituan.ptubes.reader.schema.common.ResultSetRow;
import com.meituan.ptubes.reader.schema.common.ResultSetTable;

public class Query implements Closeable {
	private final Transport transport;

	public static Transport getDefaultTransport(String host, int port, String user, String passwd) throws Exception {
		final TransportImpl r = new TransportImpl();
		r.setLevel1BufferSize(1024 * 1024);
		r.setLevel2BufferSize(8 * 1024 * 1024);

		final AuthenticatorImpl authenticator = new AuthenticatorImpl();
		authenticator.setUser(user);
		authenticator.setPassword(passwd);
		authenticator.setEncoding("utf-8");
		r.setAuthenticator(authenticator);

		final SocketFactoryImpl socketFactory = new SocketFactoryImpl();
		socketFactory.setKeepAlive(true);
		socketFactory.setSoTimeout(16);
		socketFactory.setTcpNoDelay(false);
		socketFactory.setReceiveBufferSize(1024 * 1024);
		r.setSocketFactory(socketFactory);

		r.connect(host, port);
		return r;
	}

	public Query(Transport transport) {
		this.transport = transport;
	}

	@Override public void close() throws IOException {
		try {
			transport.disconnect();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public List<String> getFirst(String sql) throws IOException, TransportException {
		List<String> result = null;

		final ComQuery command = new ComQuery();
		command.setSql(StringColumn.valueOf(sql.getBytes()));
		transport.getOutputStream().writePacket(command);
		transport.getOutputStream().flush();

		Packet packet = transport.getInputStream().readPacket();
		if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
			throw new TransportException(ErrorPacket.valueOf(packet));
		}

		ResultSetHeaderPacket header = ResultSetHeaderPacket.valueOf(packet);
		if (header.getFieldCount().longValue() == 0) {
			return null;
		}

		while (true) {
			packet = transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
				break;
			}
		}

		while (true) {
			packet = transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
				break;
			} else {
				ResultSetRowPacket row = ResultSetRowPacket.valueOf(packet);
				if (result == null) {
					result = new ArrayList<String>();

					for (StringColumn c : row.getColumns()) {
						result.add(c.toString());
					}
				}
			}
		}
		return result;
	}

	public List<String> get(String sql) throws IOException, TransportException {
		List<String> result = new ArrayList<String>();

		final ComQuery command = new ComQuery();
		command.setSql(StringColumn.valueOf(sql.getBytes()));
		transport.getOutputStream().writePacket(command);
		transport.getOutputStream().flush();

		Packet packet = transport.getInputStream().readPacket();
		if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
			throw new TransportException(ErrorPacket.valueOf(packet));
		}

		ResultSetHeaderPacket header = ResultSetHeaderPacket.valueOf(packet);
		if (header.getFieldCount().longValue() == 0) {
			return null;
		}

		while (true) {
			packet = transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
				break;
			}
		}

		while (true) {
			packet = transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
				break;
			} else {
				ResultSetRowPacket row = ResultSetRowPacket.valueOf(packet);
				for (StringColumn c : row.getColumns()) {
					if (Objects.nonNull(c)) {
						result.add(c.toString());
					} else {
						result.add(null);
					}
				}
			}
		}
		return result;
	}

	public ResultSetTable getResultSetTable(String sql) throws IOException {
		final ComQuery command = new ComQuery();
		command.setSql(StringColumn.valueOf(sql.getBytes()));
		transport.getOutputStream().writePacket(command);
		transport.getOutputStream().flush();

		Packet packet = transport.getInputStream().readPacket();
		if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
			throw new TransportException(ErrorPacket.valueOf(packet));
		}

		ResultSetHeaderPacket header = ResultSetHeaderPacket.valueOf(packet);
		if (header.getFieldCount().longValue() == 0) {
			return null;
		}

		List<String> columnNames = new ArrayList<>();
		while (true) {
			packet = transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
				break;
			} else {
				ColumnDefPacket columnDefPacket = ColumnDefPacket.valueOf(false, packet);
				columnNames.add(columnDefPacket.getOrgName().toString());
			}
		}
		ResultSetHeader resultSetHeader = new ResultSetHeader(columnNames);

		List<ResultSetRow> resultSetRows = new ArrayList<>();
		while (true) {
			packet = transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
				break;
			} else {
				ResultSetRowPacket row = ResultSetRowPacket.valueOf(packet);
				resultSetRows.add(new ResultSetRow(row.getColumns()));
			}
		}

		return new ResultSetTable(resultSetHeader, resultSetRows);
	}

	/**
	 * SHOW FULL COLUMNS FROM {tb} FROM {db}
	 * +------------------+---------------------------+--------------------+------+-----+----------------------+--------------------------------+---------------------------------+------------------------+
	 * | Field            | Type                      | Collation          | Null | Key | Default              | Extra                          | Privileges                      | Comment                |
	 * +------------------+---------------------------+--------------------+------+-----+----------------------+--------------------------------+---------------------------------+------------------------+
	 *
	 * SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {db} AND TABLE_NAME = {tb}
	 * return [TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA, PRIVILEGES, COLUMN_COMMENT, GENERATION_EXPRESSION]
	 */
	public List<ColumnMetaData> getOrderedColumnMetaData(String database, String table) throws Exception {
		List<ColumnMetaData> res = new ArrayList<>();

		final String sql = String.format("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", database, table);
		ResultSetTable resultSetTable = getResultSetTable(sql);

		for (int i = 0, rowSize = resultSetTable.getRows().size(); i < rowSize; i++) {
			String columnName = resultSetTable.getString(i, "COLUMN_NAME");
			int columnPos = resultSetTable.getInt(i, "ORDINAL_POSITION");
			// nullable
			String columnDefault = resultSetTable.getString(i, "COLUMN_DEFAULT");
			boolean columnIsNullable = "YES".equalsIgnoreCase(resultSetTable.getString(i, "IS_NULLABLE"));
			String columnType = resultSetTable.getString(i, "DATA_TYPE");
			// todo: Do zero have an ambiguous meanings while NUMERIC_PRECISION is null?
			Integer cnp = resultSetTable.getInt(i, "NUMERIC_PRECISION");
			int columnNumbericPrecise = (Objects.nonNull(cnp) ? cnp : 0);
			Integer cns = resultSetTable.getInt(i, "NUMERIC_SCALE");
			int columnNumbericScale = (Objects.nonNull(cns) ? cns : 0);
			Integer cdp = resultSetTable.getInt(i, "DATETIME_PRECISION");
			int columnDatetimePrecise = (Objects.nonNull(cdp) ? cdp : 0);
			// nullable
			String columnCharSet = resultSetTable.getString(i, "CHARACTER_SET_NAME");
			boolean columnIsUnsigned = resultSetTable.getString(i, "COLUMN_TYPE").toLowerCase().contains("unsigned");

			res.add(new ColumnMetaData(columnPos, columnName, columnDefault, columnIsNullable, columnType,
				columnNumbericPrecise, columnNumbericScale, columnDatetimePrecise, columnCharSet, columnIsUnsigned));
		}
		Collections.sort(res, (data, t1) -> (data.getPos() < t1.getPos() ? -1 : (data.getPos() > t1.getPos() ? 1 : 0)));

		return res;
	}

	/**
	 * We are using "SHOW KEYS FROM {db}.{tb}" to generate table schema.
	 * 1. SHOW KEYS FROM {tb} FROM {db}
	 * (including primary key, unique index, secondary index)
	 * +--------+------------+---------------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	 * | Table  | Non_unique(1) | Key_name(2)     | Seq_in_index(3) | Column_name(4) | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
	 * +--------+------------+---------------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	 *
	 * 2. SELECT CONSTRAINT_NAME,COLUMN_NAME,ORDINAL_POSITION FROM KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = {db} AND TABLE_NAME = {tb}
	 * (only included primary key and unique index)
	 * +-----------------+-------------+------------------+
	 * | CONSTRAINT_NAME | COLUMN_NAME | ORDINAL_POSITION |
	 * +-----------------+-------------+------------------+
	 *
	 * 3. SELECT * FROM TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = {db} AND TABLE_NAME = {tb};
	 * +--------------------+-------------------+-----------------+--------------+------------+-----------------+
	 * | CONSTRAINT_CATALOG | CONSTRAINT_SCHEMA | CONSTRAINT_NAME | TABLE_SCHEMA | TABLE_NAME | CONSTRAINT_TYPE |
	 * +--------------------+-------------------+-----------------+--------------+------------+-----------------+
	 * * The value can be UNIQUE, PRIMARY KEY, FOREIGN KEY
	 */
	public List<IndexMetaData> getIndexMetaData(String database, String table) throws Exception {
		List<IndexMetaData> res = new ArrayList<>();

		final String sql = String.format("SHOW KEYS FROM %s FROM %s", table, database);
		ResultSetTable resultSetTable = getResultSetTable(sql);

		Map<String/* index name */, TreeMap<Integer/* seq in index */, String/* col name */>/* index cols */> indexColMap = new HashMap<>();
		Map<String/* index name */, IndexMetaData> indexMetaDataMap = new HashMap<>();
		for (int i = 0, rowSize = resultSetTable.getRows().size(); i < rowSize; i++) {
			String keyName = resultSetTable.getString(i, "INDEX_NAME");
			int seqInIndex = resultSetTable.getInt(i, "SEQ_IN_INDEX");
			String columnName = resultSetTable.getString(i, "COLUMN_NAME");

			if (indexColMap.containsKey(keyName)) {
				indexColMap.get(keyName).put(seqInIndex, columnName);
			} else {
				TreeMap<Integer, String> indexCols = new TreeMap<>();
				indexCols.put(seqInIndex, columnName);
				indexColMap.put(keyName, indexCols);
			}

			if (indexMetaDataMap.containsKey(keyName)) {
				continue;
			} else {
				Integer nonUnique = resultSetTable.getInt(i, "NON_UNIQUE");
				boolean indexIsUnique = (Objects.nonNull(nonUnique) ? (0 == nonUnique) : false);
				boolean indexIsPrimary = ("PRIMARY".equalsIgnoreCase(keyName) || "PRI".equalsIgnoreCase(keyName));
				indexMetaDataMap.put(keyName, new IndexMetaData(indexIsUnique, indexIsPrimary, keyName, new ArrayList<>()));
			}
		}

		for (Map.Entry<String, IndexMetaData> indexMetaDataMapEntry : indexMetaDataMap.entrySet()) {
			String keyName = indexMetaDataMapEntry.getKey();
			IndexMetaData indexMetaData = indexMetaDataMapEntry.getValue();
			indexMetaData.addIndexCols(indexColMap.get(keyName).values());
		}
		return Lists.newArrayList(indexMetaDataMap.values());
	}

	public IndexMetaData getPrimaryIndexMetaData(String database, String table) throws Exception {
		List<IndexMetaData> res = getIndexMetaData(database, table);
		return res.stream().filter(IndexMetaData::isPrimary).findFirst().get();
	}

	public String getGtidPurged() throws Exception {
		List<String> queryResult = getFirst("show variables like 'gtid_purged'");
		if (queryResult.size() < 2) {
			return "";
		}
		return queryResult.get(1).replace("\n", "");
	}

	/**
	 *	gtid_purged + Previous_gtid_log
	 *	Confirm the previously executed gtid collection
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public GtidSet getPreviousGtids(String fileName) throws Exception {
		String purgedGtids = getGtidPurged();
		GtidSet purgedGtidSet = new GtidSet(purgedGtids);
		List<String> queryResult = get("show binlog events in '" + fileName + "' limit 2");
		GtidSet previousGtidSet;
		if (queryResult.size() < 12) {
			previousGtidSet = new GtidSet("");
		} else {
			previousGtidSet = new GtidSet(queryResult.get(11).replace("\n", ""));
		}
		return GtidSet.mergeGtidSet(purgedGtidSet, previousGtidSet);
	}

	public boolean isGtidMode() throws Exception {
		// The structure is similar to [gtid_mode,on]
		List<String> queryResult = get("show variables like 'gtid_mode'");
		if (queryResult.size() < 2) {
			return false;
		}
		return "ON".equalsIgnoreCase(queryResult.get(1));
	}

	public List<String> getBinaryLogs() throws Exception {
		final Query query = new Query(this.transport);
		// All binlog file names, the structure is similar to [mysql-bin.000545,1073742011,mysql-bin.000546,1073783451]
		List<String> queryResult = query.get("SHOW BINARY LOGS");
		List<String> result = new ArrayList<String>(); // [Log_name, File_size]?
		for (int i = 0; i < queryResult.size(); i += 2) {
			result.add(queryResult.get(i));
		}
		return result;
	}

	public Map<String, String> getBinaryLogsInfo() throws Exception {
		final Query query = new Query(this.transport);
		List<String> queryResult = query.get("SHOW BINARY LOGS");
		Map<String, String> result = new HashMap<>(); // [Log_name, File_size]?
		for (int i = 0; i < queryResult.size(); i += 2) {
			result.put(queryResult.get(i), queryResult.get(i + 1));
		}
		return result;
	}
}
