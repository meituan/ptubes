package com.meituan.ptubes.reader.producer.mysqlreplicator.component;

import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.WorkHandler;
import java.util.ArrayList;
import java.util.List;
import com.meituan.ptubes.reader.container.common.constants.ProducerConstants;
import com.meituan.ptubes.reader.container.common.vo.KeyPair;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.BinlogData;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Pair;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.context.RowContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.context.TxnContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.RecordUtil;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import com.meituan.ptubes.reader.storage.common.event.MySQLChangeEntry;
import com.meituan.ptubes.sdk.protocol.RdsPacket;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public class RowParser implements WorkHandler<BinlogData>, TimeoutHandler, LifecycleAware {

    private static final Logger LOG = LoggerFactory.getLogger(RowParser.class);

    private final BinlogPipeline parent;

    public RowParser(BinlogPipeline parent) {
        this.parent = parent;
    }

    @Override public void onEvent(BinlogData event) throws Exception {
        if (parent.isShutdownRequested()) {
            return;
        }

        TxnContext txnContext = event.getTxnContext();
        RowContext rowContext = event.getRowContext();
        Pair<Row> rowPair = event.getRow();
        try {
            MySQLBinlogInfo binlogInfo = new MySQLBinlogInfo(parent.changeId, parent.rdsConfig.getServerId(), txnContext.getBinlogId(),
                txnContext.getBinlogOffset(), txnContext.getGtid().getUuid(), txnContext.getGtid().getTransactionId(), event.getEventIndex(), txnContext.getTxnProduceTimeMs());
            binlogInfo.setIncrementalLabel(event.getAutoIncrLabel());
            List<KeyPair> kps = new ArrayList<>();
            RdsPacket.RdsEvent ptubesEvent = RecordUtil.generatePtubesEvent(
                parent.eventProducer.getReaderTaskName(),
                kps,
                binlogInfo,
                rowContext.getSchema(),
                rowContext.getType(),
                rowContext.getTableName(),
                rowPair,
                rowContext.getEventProduceTimeMs(),
                rowContext.getEventReceiveTimeMs(),
                rowContext.getComments(),
                rowContext.getSql(),
                parent.fromIp
            );

            EventType eventType = rowContext.getType();
            if (RdsPacket.EventType.HEARTBEAT.equals(ptubesEvent.getEventType())) {
                eventType = EventType.HEARTBEAT;
                logHeartbeatInfo(binlogInfo, ptubesEvent);
            }
            MySQLChangeEntry dbEntry = new MySQLChangeEntry(rowContext.getTableName(), parent.fromIp, binlogInfo, rowContext.getEventProduceTimeMs(),
                rowContext.getEventPipelineReceiveTimeNs(), txnContext.getGtidSet(), eventType, kps, ptubesEvent);
            byte[] serializedChangeEntry = dbEntry.getSerializedChangeEntry();
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} producer change entry, size is {} bytes", parent.eventProducer.getReaderTaskName(), serializedChangeEntry.length);
            }
            event.setChangeEntry(dbEntry);
        } catch (Throwable te) {
            LOG.error("{} parse row data error, skip row data at txn = {}, row = {}", parent.eventProducer.getReaderTaskName(),
                event.getTxnContext().toString(), event.getRowContext().toString(), te);
            event.setSkipable(true);
        }
    }

    private void logHeartbeatInfo(MySQLBinlogInfo binlogInfo, RdsPacket.RdsEvent rdsEvent) {
        long heartbeatTimeMS = Long.valueOf(rdsEvent.getRowData().getAfterColumnsMap().get(ProducerConstants.HEARTBEAT_TABLE_UTIME_COL_NAME).getValue());
        parent.readerTaskStatMetricsCollector.setLastHeartBeatTime(heartbeatTimeMS);
        parent.readerTaskStatMetricsCollector.setLastHeartBeatBinlogInfo(binlogInfo);
        long currTs = System.currentTimeMillis();
        parent.readerTaskStatMetricsCollector.setHeartbeatRecieveTimestamp(currTs);
    }

    @Override public void onStart() {
        LOG.info("{} Binlog Row Parser start!", parent.eventProducer.getReaderTaskName());
    }

    @Override public void onShutdown() {
        LOG.info("{} Binlog Row Parser shutdown!", parent.eventProducer.getReaderTaskName());
    }

    @Override public void onTimeout(long sequence) throws Exception {
        LOG.info("{} Binlog Row Parser waitfor new data timeout!", parent.eventProducer.getReaderTaskName());
    }
}
