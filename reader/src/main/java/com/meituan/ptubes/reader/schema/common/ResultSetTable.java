package com.meituan.ptubes.reader.schema.common;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import java.util.List;
import java.util.Objects;

public class ResultSetTable {

    private final ResultSetHeader header;
    private final List<ResultSetRow> rows;

    public ResultSetTable(ResultSetHeader header,
        List<ResultSetRow> rows) {
        this.header = header;
        this.rows = rows;
    }

    public ResultSetHeader getHeader() {
        return header;
    }

    public List<ResultSetRow> getRows() {
        return rows;
    }

    public StringColumn getColumn(int rowIndex, String columnName) {
        int columnIndex = findColumnIndex(columnName);
        if (columnIndex == -1) {
            return null;
        }
        return rows.get(rowIndex).getRowValue().get(columnIndex);
    }

    // easy-use
    public String getString(int rowIndex, String columnName) {
        StringColumn sc = getColumn(rowIndex, columnName);
        return (Objects.nonNull(sc) ? sc.toString() : null);
    }

    public Integer getInt(int rowIndex, String columnName) {
        StringColumn sc = getColumn(rowIndex, columnName);
        return (Objects.nonNull(sc) ? Integer.valueOf(sc.toString()) : null);
    }

    public Long getLong(int rowIndex, String columnName) {
        StringColumn sc = getColumn(rowIndex, columnName);
        return (Objects.nonNull(sc) ? Long.valueOf(sc.toString()) : null);
    }

    public byte[] getBytes(int rowIndex, String columnName) {
        StringColumn sc = getColumn(rowIndex, columnName);
        return (Objects.nonNull(sc) ? sc.getValue() : null);
    }

    // todo: cache column index for specific column name
    private int findColumnIndex(String columnName) {
        Integer index = header.getColumnIndex().get(columnName);
        return (Objects.nonNull(index) ? index : -1);
    }

}
