package com.meituan.ptubes.reader.container.common.constants;


public class ProducerConstants {
	public static final String CONF_BASE_DIR =  "metaFile";
	public static final String SCHEMA_BASE_DIR =  "schema";
	public static final String CHANGE_ID_INFO_FILE_NAME = "changeIdInfo";
	public static final String MAX_BINLOG_INFO_FILE_NAME = "maxBinlogInfo";

	/** Insert event count **/
	public static final String CAT_SUBSCRIBE_INSERT_TYPE = "Ptubes.SubscribeEvent.Insert";
	/** Update event count **/
	public static final String CAT_SUBSCRIBE_UPDATE_TYPE = "Ptubes.SubscribeEvent.Update";
	/** delete event count **/
	public static final String CAT_SUBSCRIBE_DELETE_TYPE = "Ptubes.SubscribeEvent.Delete";
	public static final String CAT_SUBSCRIBE_DDL_TYPE = "Ptubes.SubscribeEvent.DDL";
	/** Sub-reader tasks, received event management **/
	public static final String CAT_EVENT_LISTENER_TYPE = "Ptubes.EventListener.EventReceiveCount";
	public static final String CAT_BIG_EVENT_TYPE_PRE = "Ptubes.EventPackageSize";
	public static final String CAT_HEARTBEAT_LATENCY_TYPE = "Ptubes.Heartbeat.Latency";
	/** Time of receiving binlog - time of binlog generation. It is equal to master-slave delay + reader pull consumption + reader pull delay **/
	public static final String CAT_EVENT_RECEIVE_LATENCY_TYPE = "Ptubes.EventListener.ReceiveLatency";

	public static final String COMMIT_DB_TABLE_NAME = "__controlDB__.__commit__";
	public static final String HEARTBEAT_SCHEMA = "buffalo_heartbeat";
	public static final String HEARTBEAT_TABLE = "heartbeat";
	public static final String HEARTBEAT_DB_TABLE_NAME = HEARTBEAT_SCHEMA + "." + HEARTBEAT_TABLE;
	public static final String HEARTBEAT_TABLE_ID_COL_NAME = "id";
	public static final String HEARTBEAT_TABLE_UTIME_COL_NAME = "utime";
}
