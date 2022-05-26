package com.meituan.ptubes.reader.producer.mysqlreplicator.component;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.SchemaParseException;
import com.meituan.ptubes.reader.container.common.constants.ProducerConstants;
import com.meituan.ptubes.reader.container.common.vo.Gtid;
import com.meituan.ptubes.reader.producer.common.StreamableThreadBase;
import com.meituan.ptubes.reader.producer.mysqlreplicator.ControlEventProducer;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.BinlogData;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Pair;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.context.RowContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.context.TxnContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.DeleteRowsEventV2;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.GtidEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.QueryEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.RotateEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.RowsQueryEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.TableMapEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.UpdateRowsEventV2;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.WriteRowsEventV2;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.XidEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.SqlParseUtil;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;

import static com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants.DELETE_ROWS_EVENT_V2;
import static com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants.GTID_LOG_EVENT;
import static com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants.QUERY_EVENT;
import static com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants.ROTATE_EVENT;
import static com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants.ROWS_QUERY_LOG_EVENT;
import static com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants.TABLE_MAP_EVENT;
import static com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants.UPDATE_ROWS_EVENT_V2;
import static com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants.WRITE_ROWS_EVENT_V2;
import static com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants.XID_EVENT;
import static com.meituan.ptubes.reader.schema.common.VirtualSchema.INTERNAL_TABLE_SCHEMA;
import static com.meituan.ptubes.reader.storage.common.event.EventType.COMMIT;
import static com.meituan.ptubes.reader.storage.common.event.EventType.DDL;
import static com.meituan.ptubes.reader.storage.common.event.EventType.DELETE;
import static com.meituan.ptubes.reader.storage.common.event.EventType.INSERT;
import static com.meituan.ptubes.reader.storage.common.event.EventType.UPDATE;

public class BinlogContextParser extends StreamableThreadBase<BinlogEventV4, Void> {

    private final BinlogPipeline parent;
    private final RingBuffer<BinlogData> stageBuffer;
    private int currBinlogId;
    private int eventIndex = -1;
    private boolean firstTxn = false;
    private boolean receiveGtidEvent = false;
    private final Map<Long, String> tableNameMap = new HashMap<>();
    private TxnContext txnContext;
    private RowContext rowContext;

    public BinlogContextParser(BinlogPipeline parent, int queueSize) {
        super(parent.eventProducer.getReaderTaskName() + "-ContextParser", queueSize);
        this.parent = parent;
        this.stageBuffer = parent.binlogDataBuffer;
        this.currBinlogId = parent.startAtMaxBinlogInfo.getBinlogId();
    }

    @Override protected Void processEvent(BinlogEventV4 binlogEvent) throws Exception {
        processEventInternal(binlogEvent);
        processEventIfNewTxnBegin(binlogEvent);
        return null;
    }

    @Override protected void prepareToStartup() throws Exception {
        ThreadContext.put("TASKNAME", parent.eventProducer.getReaderTaskName());
    }

    @Override protected void prepareToShutdown() throws Exception {
        txnContext = null;
        rowContext = null;
    }

    /**
     * DML: | GtidEvent(gtidMode=On)                        |  TxnContext
     *      | QueryEvent(DDL)                               |  TxnContext
     *      | RowsQueryEvent(sql) TableMapEvent RowsEvent   |  RowContext
     *      | RowsQueryEvent TableMapEvent RowsEvent        |  RowContext
     *      | XidEvent ... RotateEvent                      |
     * DDL: | GtidEvent(gtidMode=On)                        |  TxnContext
     *      | QueryEvent(DDL)                               |  TxnContext
     * @param binlogEvent
     */
    private void processEventInternal(BinlogEventV4 binlogEvent) {
        switch (binlogEvent.getHeader().getEventType()) {
            case GTID_LOG_EVENT:
                processGtidEvent((GtidEvent) binlogEvent);
                break;
            case ROTATE_EVENT:
                processRotateEvent((RotateEvent) binlogEvent);
                break;
            case QUERY_EVENT:
                processQueryEvent((QueryEvent) binlogEvent);
                break;
            default:
                break;
        }
    }
    private void processEventIfNewTxnBegin(BinlogEventV4 binlogEvent) throws Exception {
        if (firstTxn) {
            switch (binlogEvent.getHeader().getEventType()) {
                case ROWS_QUERY_LOG_EVENT:
                    processRowsQueryEvent((RowsQueryEvent) binlogEvent);
                    break;
                case TABLE_MAP_EVENT:
                    processTableMapEvent((TableMapEvent) binlogEvent);
                    break;
                case XID_EVENT:
                    processXidEvent((XidEvent) binlogEvent);
                    break;
                case WRITE_ROWS_EVENT_V2:
                    processWriteRowsEvent((WriteRowsEventV2) binlogEvent);
                    break;
                case UPDATE_ROWS_EVENT_V2:
                    processUpdateRowsEvent((UpdateRowsEventV2) binlogEvent);
                    break;
                case DELETE_ROWS_EVENT_V2:
                    processDeleteRowsEvent((DeleteRowsEventV2) binlogEvent);
                    break;
                case GTID_LOG_EVENT:
                case ROTATE_EVENT:
                case QUERY_EVENT:
                    break;
                default:
                    if (log.isDebugEnabled()) {
                        log.debug("Skipping event: {}", binlogEvent.toString());
                    }
            }
        } else {
            // do nothing
        }
    }
    private void publishBinlogData(TxnContext txnContext, RowContext rowContext, EventType type, int eventIndex, Pair<Row> row) {
        while (!parent.isShutdownRequested()) {
            try {
                long nextValidBufferPos = stageBuffer.tryNext();
                BinlogData binlogData = stageBuffer.get(nextValidBufferPos);
                binlogData.setSkipable(false);
                binlogData.setTxnContext(TxnContext.clone(txnContext));
                binlogData.setRowContext(RowContext.clone(rowContext));
                binlogData.setType(type);
                binlogData.setRow(row);
                binlogData.setEventIndex(eventIndex);
                binlogData.setChangeEntry(null);
                binlogData.setAutoIncrLabel(parent.sequencer.nextSequence().getSequenceId());
                stageBuffer.publish(nextValidBufferPos);
                break;
            } catch (InsufficientCapacityException ice) {
                log.debug("{} binlog buffer has no enough capacity", parent.eventProducer.getReaderTaskName());
                // release cpu
                Thread.yield();
            }
        }
    }
    private void publishQueryEvent(QueryEvent queryEvent, String fullTableName, String ddlSql) {
        try {
            log.info("table changed {} at (scn){}.{}, (time){}", queryEvent, txnContext.getBinlogId(), txnContext.getBinlogOffset(), txnContext.getTxnProduceTimeMs());
            List<Pair<Row>> pl = ControlEventProducer.genTableChangedPair(fullTableName, ddlSql);

            for (Pair<Row> rowPair : pl) {
                eventIndex++;
                publishBinlogData(txnContext, rowContext, DDL, eventIndex, rowPair);
            }
        } catch (Exception exp) {
            log.error("publish table {} ddl event {} error", fullTableName, queryEvent, exp);
        }
    }
    private void publishWriteRowsEvent(WriteRowsEventV2 writeRowsEvent) {
        List<Row> rls = writeRowsEvent.getRows();
        for (Row r : rls) {
            eventIndex++;
            Pair<Row> rowPair = new Pair<>(null, r);
            publishBinlogData(txnContext, rowContext, INSERT, eventIndex, rowPair);
        }
    }
    private void publishUpdateRowsEvent(UpdateRowsEventV2 updateRowsEvent) {
        for (Pair<Row> rowPair : updateRowsEvent.getRows()) {
            eventIndex++;
            publishBinlogData(txnContext, rowContext, UPDATE, eventIndex, rowPair);
        }
    }
    private void publishDeleteRowsEvent(DeleteRowsEventV2 deleteRowsEvent) {
        List<Row> rls = deleteRowsEvent.getRows();
        for (Row r : rls) {
            eventIndex++;
            Pair<Row> rowPair = new Pair<>(r, null);
            publishBinlogData(txnContext, rowContext, DELETE, eventIndex, rowPair);
        }
    }
    private void publishXidEvent(XidEvent xidEvent) {
        try {
            List<Pair<Row>> pl = ControlEventProducer.genCommitPair();
            for (Pair<Row> rowPair : pl) {
                eventIndex++;
                publishBinlogData(txnContext, rowContext, COMMIT, eventIndex, rowPair);
            }
        } catch (Exception exp) {
            log.error("publish xid event {} error", xidEvent.toString(), exp);
        }
    }
    // reset TxnContext
    // todo: add function called resetTxnContext() resetRowContext()
    private void processGtidEvent(GtidEvent gtidEvent) {
        eventIndex = -1;
        txnContext = new TxnContext(currBinlogId);
        txnContext.setGtid(new Gtid(gtidEvent.getUuid(), gtidEvent.getTransactionId()));
        txnContext.setGtidSet(gtidEvent.getPrevGtidSet());
        txnContext.setBinlogOffset(gtidEvent.getHeader().getPosition());
        txnContext.setTxnProduceTimeMs(gtidEvent.getHeader().getTimestamp());
        receiveGtidEvent = true;
    }
    // reset TxnContext if no GtidEvent
    private void processQueryEvent(QueryEvent queryEvent) {
        String sql = SqlParseUtil.getSqlFromEvent(queryEvent.getSql().toString());
        SqlParseUtil.QUERY_EVENT_TYPE queryEventType = SqlParseUtil.getQueryEventType(sql);

        if (receiveGtidEvent == false) {
            // If gtidMode is not enabled, txnContext needs to be updated in the begin statement
            eventIndex = -1;
            txnContext = new TxnContext(currBinlogId);
            txnContext.setBinlogOffset(queryEvent.getHeader().getPosition());
            txnContext.setTxnProduceTimeMs(queryEvent.getHeader().getTimestamp());
        }
        if (SqlParseUtil.QUERY_EVENT_TYPE.BEGIN == queryEventType) {
            firstTxn = true;
            if (txnContext.getBinlogId() <= 0) {
                String fileName = queryEvent.getBinlogFilename();
                String binlogFilePrefix = parent.eventProducer.getProducerConfig().getProducerBaseConfig().getBinlogFilePrefix();
                String fileNumStr = fileName.substring(
                    fileName.lastIndexOf(binlogFilePrefix) + binlogFilePrefix.length() + 1);
                txnContext.setBinlogId(Integer.parseInt(fileNumStr));
            }
        } else if (SqlParseUtil.QUERY_EVENT_TYPE.RENAME == queryEventType) {
            renameTable(queryEvent, sql);
        } else if (SqlParseUtil.QUERY_EVENT_TYPE.ALTER == queryEventType) {
            alterTable(queryEvent, sql);
        } else if (SqlParseUtil.QUERY_EVENT_TYPE.CREATE == queryEventType) {
            createTable(queryEvent, sql);
        } else {
            // do nothing
        }
    }
    private void renameTable(QueryEvent event, String sql) {
        log.info("RenameTable sql: " + sql);
        String[] renameList = sql.split("\\s*,\\s*");
        for (int i = 0; i < renameList.length; i++) {
            String dbTableName = SqlParseUtil.parseDbTableNameFromRenameTableSql(event.getDatabaseName().toString(), renameList[i]);

            if (StringUtils.isBlank(dbTableName) || !isConcernTable(dbTableName)) {
                continue;
            }
            try {
                rowContext = new RowContext();
                rowContext.setTableName(dbTableName);
                rowContext.setType(DDL);
                rowContext.setSql(sql);
                rowContext.setSchema(INTERNAL_TABLE_SCHEMA);
                rowContext.setEventProduceTimeMs(event.getHeader().getTimestamp());
                rowContext.setEventReceiveTimeMs(event.getHeader().getReplicatorInboundTS());
                rowContext.setEventPipelineReceiveTimeNs(System.nanoTime());
                parent.schemaProvider.refreshTableSchema(dbTableName, event.getHeader().getTimestamp());
                publishQueryEvent(event, dbTableName, sql);
            } catch (Exception e) {
                log.error("Refresh schema error, tableName: {}", dbTableName, e);
            }
        }
    }
    private void alterTable(QueryEvent event, String sql) {
        String dbTableName = SqlParseUtil.parseDbTableNameFromAlterTableSql(event.getDatabaseName().toString(), sql);

        if (StringUtils.isBlank(dbTableName) || !isConcernTable(dbTableName)) {
            return;
        }
        try {
            rowContext = new RowContext();
            rowContext.setTableName(dbTableName);
            rowContext.setType(DDL);
            rowContext.setSql(sql);
            rowContext.setSchema(INTERNAL_TABLE_SCHEMA);
            parent.schemaProvider.refreshTableSchema(dbTableName, event.getHeader().getTimestamp());
            publishQueryEvent(event, dbTableName, sql);
        } catch (Exception e) {
            log.error("Refresh schema error, tableName: {}", dbTableName, e);
        }
    }
    private void createTable(QueryEvent event, String sql) {
        String dbTableName = SqlParseUtil.parseDbTableNameFromCreateSql(event.getDatabaseName().toString(), sql);

        if (StringUtils.isBlank(dbTableName) || !isConcernTable(dbTableName)) {
            return;
        }
        try {
            rowContext = new RowContext();
            rowContext.setTableName(dbTableName);
            rowContext.setType(DDL);
            rowContext.setSql(sql);
            rowContext.setSchema(INTERNAL_TABLE_SCHEMA);
            parent.schemaProvider.refreshTableSchema(dbTableName, event.getHeader().getTimestamp());
            publishQueryEvent(event, dbTableName, sql);
        } catch (Exception e) {
            log.error("Refresh schema error, tableName: {}", dbTableName, e);
        }
    }

    // reset RowContext
    private void processRowsQueryEvent(RowsQueryEvent rowsQueryEvent) {
        rowContext = new RowContext();
        String sql = rowsQueryEvent.getQueryText().toString();
        if (parent.producerConfigService.getConfig().getProducerBaseConfig().isEnableStoreSQL()) {
            rowContext.setSql(sql);
        }
        rowContext.setComments(SqlParseUtil.getCommentV2(sql));
    }
    private void processTableMapEvent(TableMapEvent tableMapEvent) {
        String fullTableName = tableNameMap.get(tableMapEvent.getTableId());
        if (Objects.isNull(fullTableName)) {
            fullTableName = tableMapEvent.getDatabaseName().toString() + "." + tableMapEvent.getTableName().toString();
            tableNameMap.putIfAbsent(tableMapEvent.getTableId(), fullTableName);
            rowContext.setTableName(fullTableName);
        } else {
            rowContext.setTableName(fullTableName);
        }
    }
    private void processWriteRowsEvent(WriteRowsEventV2 writeRowsEvent) throws PtubesException, SchemaParseException, IOException {
        if (StringUtils.isBlank(rowContext.getTableName()) || !isConcernTable(rowContext.getTableName())) {
            return;
        }
        rowContext.setType(INSERT);
        rowContext.setEventProduceTimeMs(writeRowsEvent.getHeader().getTimestamp());
        rowContext.setEventReceiveTimeMs(writeRowsEvent.getHeader().getReplicatorInboundTS());
        rowContext.setEventPipelineReceiveTimeNs(System.nanoTime());
        rowContext.setSchema(parent.schemaProvider.loadTableSchemaFromCache(rowContext.getTableName(), writeRowsEvent.getHeader().getTimestamp()));
        publishWriteRowsEvent(writeRowsEvent);
    }
    private void processUpdateRowsEvent(UpdateRowsEventV2 updateRowsEvent) throws PtubesException, SchemaParseException, IOException {
        if (StringUtils.isBlank(rowContext.getTableName()) || !isConcernTable(rowContext.getTableName())) {
            return;
        }
        rowContext.setType(UPDATE);
        rowContext.setEventProduceTimeMs(updateRowsEvent.getHeader().getTimestamp());
        rowContext.setEventReceiveTimeMs(updateRowsEvent.getHeader().getReplicatorInboundTS());
        rowContext.setEventPipelineReceiveTimeNs(System.nanoTime());
        rowContext.setSchema(parent.schemaProvider.loadTableSchemaFromCache(rowContext.getTableName(), updateRowsEvent.getHeader().getTimestamp()));
        publishUpdateRowsEvent(updateRowsEvent);
    }
    private void processDeleteRowsEvent(DeleteRowsEventV2 deleteRowsEvent) throws PtubesException, SchemaParseException, IOException {
        if (StringUtils.isBlank(rowContext.getTableName()) || !isConcernTable(rowContext.getTableName())) {
            return;
        }
        rowContext.setType(DELETE);
        rowContext.setEventProduceTimeMs(deleteRowsEvent.getHeader().getTimestamp());
        rowContext.setEventReceiveTimeMs(deleteRowsEvent.getHeader().getReplicatorInboundTS());
        rowContext.setEventPipelineReceiveTimeNs(System.nanoTime());
        rowContext.setSchema(parent.schemaProvider.loadTableSchemaFromCache(rowContext.getTableName(), deleteRowsEvent.getHeader().getTimestamp()));
        publishDeleteRowsEvent(deleteRowsEvent);
    }
    // clear context & produce a commit event
    private void processXidEvent(XidEvent xidEvent) {
        receiveGtidEvent = false;
        if (eventIndex < 0) {
            return;
        }

        rowContext = new RowContext();
        rowContext.setTableName(ProducerConstants.COMMIT_DB_TABLE_NAME);
        rowContext.setType(COMMIT);
        rowContext.setSchema(INTERNAL_TABLE_SCHEMA);
        rowContext.setEventProduceTimeMs(xidEvent.getHeader().getTimestamp());
        rowContext.setEventReceiveTimeMs(xidEvent.getHeader().getReplicatorInboundTS());
        rowContext.setEventPipelineReceiveTimeNs(System.nanoTime());
        publishXidEvent(xidEvent);

        txnContext = null;
        rowContext = null;
        eventIndex = -1;
    }

    private void processRotateEvent(RotateEvent rotateEvent) {
        String fileName = rotateEvent.getBinlogFileName().toString();
        log.info("File Rotated : FileName :" + fileName);
        String binlogFilePrefix = parent.eventProducer.getProducerConfig().getProducerBaseConfig().getBinlogFilePrefix();
        String fileNumStr = fileName.substring(fileName.lastIndexOf(binlogFilePrefix) + binlogFilePrefix.length() + 1);
        currBinlogId = Integer.parseInt(fileNumStr);
    }

    private boolean isConcernTable(String dbTableName) {
        return parent.producerConfigService.allow(dbTableName);
    }
    private boolean isHeartbeatTable(String dbTableName) {
        return ProducerConstants.HEARTBEAT_DB_TABLE_NAME.equals(dbTableName);
    }
}
