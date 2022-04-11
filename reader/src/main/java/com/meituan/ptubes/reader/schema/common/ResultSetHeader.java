package com.meituan.ptubes.reader.schema.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultSetHeader {

    private final List<String> columnName;
    private final Map<String, Integer> columnIndex;

    public ResultSetHeader(List<String> columnName) {
        this.columnName = columnName;
        this.columnIndex = new HashMap<>();
        int index = 0;
        for (String cn : columnName) {
            columnIndex.put(cn, index);
            index++;
        }
    }

    public List<String> getColumnName() {
        return columnName;
    }

    public Map<String, Integer> getColumnIndex() {
        return columnIndex;
    }
}
