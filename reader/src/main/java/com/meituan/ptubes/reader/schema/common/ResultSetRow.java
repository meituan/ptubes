package com.meituan.ptubes.reader.schema.common;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import java.util.List;

public class ResultSetRow {

    private final List<StringColumn> rowValue;

    public ResultSetRow(List<StringColumn> rowValue) {
        this.rowValue = rowValue;
    }

    public List<StringColumn> getRowValue() {
        return rowValue;
    }
}
