package com.meituan.ptubes.reader.producer.mysqlreplicator.common;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Pair;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.context.RowContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.context.TxnContext;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.storage.common.event.EventType;

public class BinlogData {

    private volatile boolean skipable = false;
    private volatile TxnContext txnContext = null;
    private volatile RowContext rowContext = null;
    private volatile EventType type = null;
    private volatile Pair<Row> row = null;
    private volatile int eventIndex = 0;
    private volatile ChangeEntry changeEntry = null;
    private volatile long autoIncrLabel = 0;

    public boolean isSkipable() {
        return skipable;
    }

    public void setSkipable(boolean skipable) {
        this.skipable = skipable;
    }

    public TxnContext getTxnContext() {
        return txnContext;
    }

    public void setTxnContext(TxnContext txnContext) {
        this.txnContext = txnContext;
    }

    public RowContext getRowContext() {
        return rowContext;
    }

    public void setRowContext(RowContext rowContext) {
        this.rowContext = rowContext;
    }

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public Pair<Row> getRow() {
        return row;
    }

    public void setRow(Pair<Row> row) {
        this.row = row;
    }

    public int getEventIndex() {
        return eventIndex;
    }

    public void setEventIndex(int eventIndex) {
        this.eventIndex = eventIndex;
    }

    public ChangeEntry getChangeEntry() {
        return changeEntry;
    }

    public void setChangeEntry(ChangeEntry changeEntry) {
        this.changeEntry = changeEntry;
    }

    public long getAutoIncrLabel() {
        return autoIncrLabel;
    }

    public void setAutoIncrLabel(long autoIncrLabel) {
        this.autoIncrLabel = autoIncrLabel;
    }
}
