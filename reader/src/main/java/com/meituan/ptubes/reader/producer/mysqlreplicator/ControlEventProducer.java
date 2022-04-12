package com.meituan.ptubes.reader.producer.mysqlreplicator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Int24Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Pair;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.storage.common.event.EventType;

public class ControlEventProducer {
	public static List<Pair<Row>> genCommitPair() {
		return genPair(EventType.COMMIT, "");
	}

	public static List<Pair<Row>> genTableChangedPair(String dbTableName, String sql) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("dbTableName", dbTableName);
		map.put("sql", sql);
		return genPair(EventType.DDL, map.toString());
	}

	private static List<Pair<Row>> genPair(EventType eventType, String value) {
		if (value == null) {
			value = "";
		}
		List<Column> colList = new ArrayList<Column>();
		colList.add(Int24Column.valueOf(0));
		colList.add(Int24Column.valueOf(eventType.getCode()));
		colList.add(StringColumn.valueOf(value.getBytes()));
		Row after = new Row(colList);
		Pair<Row> pair = new Pair<Row>(null, after);
		return Arrays.asList(pair);
	}
}
