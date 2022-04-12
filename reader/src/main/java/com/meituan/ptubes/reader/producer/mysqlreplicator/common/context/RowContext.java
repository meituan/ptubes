package com.meituan.ptubes.reader.producer.mysqlreplicator.common.context;

import javax.annotation.concurrent.NotThreadSafe;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;
import com.meituan.ptubes.reader.storage.common.event.EventType;

/**
 * statement context
 */
@NotThreadSafe
public class RowContext {

    private String tableName = null;
    private EventType type = null;
    private String sql = "";
    private String comments = "";
    private VersionedSchema schema = null;
    private long eventProduceTimeMs;
    private long eventReceiveTimeMs;
    private long eventPipelineReceiveTimeNs;

    public RowContext() {
    }

    public RowContext(String tableName, EventType type, String sql, String comments, VersionedSchema schema, long eventProduceTimeMs,
        long eventReceiveTimeMs, long eventPipelineReceiveTimeNs) {
        this.tableName = tableName;
        this.type = type;
        this.sql = sql;
        this.comments = comments;
        this.schema = schema;
        this.eventProduceTimeMs = eventProduceTimeMs;
        this.eventReceiveTimeMs = eventReceiveTimeMs;
        this.eventPipelineReceiveTimeNs = eventPipelineReceiveTimeNs;
    }

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public VersionedSchema getSchema() {
        return schema;
    }

    public void setSchema(VersionedSchema schema) {
        this.schema = schema;
    }

    public long getEventProduceTimeMs() {
        return eventProduceTimeMs;
    }

    public void setEventProduceTimeMs(long eventProduceTimeMs) {
        this.eventProduceTimeMs = eventProduceTimeMs;
    }

    public long getEventReceiveTimeMs() {
        return eventReceiveTimeMs;
    }

    public void setEventReceiveTimeMs(long eventReceiveTimeMs) {
        this.eventReceiveTimeMs = eventReceiveTimeMs;
    }

    public long getEventPipelineReceiveTimeNs() {
        return eventPipelineReceiveTimeNs;
    }

    public void setEventPipelineReceiveTimeNs(long eventPipelineReceiveTimeNs) {
        this.eventPipelineReceiveTimeNs = eventPipelineReceiveTimeNs;
    }

    public static RowContext clone(RowContext origin) {
        return new RowContext(origin.tableName, origin.type, origin.sql, origin.comments, origin.schema, origin.eventProduceTimeMs,
            origin.eventReceiveTimeMs, origin.eventPipelineReceiveTimeNs);
    }

    @Override
    public String toString() {
        ToStringBuilder stringBuilder = new ToStringBuilder(this);
        stringBuilder.append("table", tableName).append("type", type);
        return stringBuilder.toString();
    }
}
