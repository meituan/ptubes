package com.meituan.ptubes.reader.producer.mysqlreplicator.common.context;

public class GlobalContext {

    private final short changId;
    private final int binlogId;

    public GlobalContext(short changId, int binlogId) {
        this.changId = changId;
        this.binlogId = binlogId;
    }

    public short getChangId() {
        return changId;
    }

    public int getBinlogId() {
        return binlogId;
    }
}
