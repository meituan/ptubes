package com.meituan.ptubes.reader.producer.mysqlreplicator.common.context;

import javax.annotation.concurrent.NotThreadSafe;
import com.meituan.ptubes.reader.container.common.vo.Gtid;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

/**
 * Transaction context
 */
@NotThreadSafe
public class TxnContext {

    private int binlogId = -1;
    private long binlogOffset = -1;
    private Gtid gtid;
    private String gtidSet;
    private long txnProduceTimeMs;

    public TxnContext(int binlogId) {
        this.binlogId = binlogId;
    }

    public TxnContext(int binlogId, long binlogOffset, Gtid gtid, String gtidSet, long txnProduceTimeMs) {
        this.binlogId = binlogId;
        this.binlogOffset = binlogOffset;
        this.gtid = gtid;
        this.gtidSet = gtidSet;
        this.txnProduceTimeMs = txnProduceTimeMs;
    }

    public int getBinlogId() {
        return binlogId;
    }

    public void setBinlogId(int binlogId) {
        this.binlogId = binlogId;
    }

    public long getBinlogOffset() {
        return binlogOffset;
    }

    public void setBinlogOffset(long binlogOffset) {
        this.binlogOffset = binlogOffset;
    }

    public Gtid getGtid() {
        return gtid;
    }

    public void setGtid(Gtid gtid) {
        this.gtid = gtid;
    }

    public String getGtidSet() {
        return gtidSet;
    }

    public void setGtidSet(String gtidSet) {
        this.gtidSet = gtidSet;
    }

    public long getTxnProduceTimeMs() {
        return txnProduceTimeMs;
    }

    public void setTxnProduceTimeMs(long txnProduceTimeMs) {
        this.txnProduceTimeMs = txnProduceTimeMs;
    }

    public static TxnContext clone(TxnContext origin) {
        return new TxnContext(origin.binlogId, origin.binlogOffset, origin.gtid, origin.gtidSet, origin.txnProduceTimeMs);
    }

    @Override
    public String toString() {
        ToStringBuilder stringBuilder = new ToStringBuilder(this);
        stringBuilder.append("binlogId", binlogId).append("binlogOffset", binlogOffset).append("gtid", gtid.toString()).append("produceTime", txnProduceTimeMs);
        return stringBuilder.toString();
    }
}
