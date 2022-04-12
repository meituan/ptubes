package com.meituan.ptubes.reader.container.common.constants;

import org.apache.commons.lang3.tuple.Pair;

public class ContainerConstants {

	/*network*/
	/*socket*/
	public static final int DATA_SERVER_PORT = 28332;
	public static final int MONITOR_SERVER_PORT = 23333;
	public static final int SO_BACKLOG = 16;
	public static final boolean SO_REUSEADDR = true;
	public static final boolean SO_KEEPALIVE = true;
	public static final boolean TCP_NODELAY = true;

	/*channel option*/
	public static final int CHANNEL_IDLE_TIMEOUT = 30;  		// second
	public static final int CHANNEL_WRITE_IDLE_TIMEOUT = 60; 	// second
	public static final int CHANNEL_READ_IDLE_TIMEOUT = 60;		// second

	/*channel attribute key name*/
	public static final String ATTR_CLIENTID = "clientId"; // for channel <-> session
	public static final String ATTR_CLIENTSESSION_COLLECTOR = "clientSessionCollector";

	/*http*/
	public static final int MAX_INITIAL_LINE_LENGTH = 4096;
	public static final int MAX_HEADER_SIZE = 8192;
	public static final int MAX_CHUNK_SIZE = 8192;
	public static final int MAX_CONTENT_LENGTH = 8 * 1024 * 1024;
	/*network end*/

	/*session*/
	public static final int MAX_EVENT_CACHE_NUM = 200;
	public static final int MAX_EVENT_CACHE_SIZE = 2 * 1024 * 1024;

	/*config*/
	/*storage belongs to one readTask*/
	public static final String STORAGE_BASIC_CONFIG = "basic";
	public static final String STORAGE_FILE_CONFIG = "file";
	public static final String STORAGE_MEM_CONFIG = "mem";

	/*readTask/producer*/
	public static final String READTASK_BASIC_CONFIG = "basic";
	public static final String READTASK_RDS_CONFIG = "rds";
	public static final String READTASK_CDC_CONFIG = "cdc";
	public static final String READTASK_CDC_EXPER_CONFIG = "cdcExper";
	public static final String READTASK_SUBSCRIPTION_CONFIG = "subs";
	public static final String READTASK_SUBSCRIPTION_DBS = "dbs";
	public static final String READTASK_SUBSCRIPTION_TABLES = "tables";

	/*server config belongs to container, global*/
	public static final String CONTAINER_READTASK_SET = "readTaskSet";
	/*config end*/

	/*schema*/
	public static final long SCHEMA_FILE_EXPIRED_TIME = 30 * 24 * 3600;
	public static final long CLEAN_MARK_SCHEMA_FILE_EXPIRED_TIME = 7 * 24 * 3600;
	/*schema end*/

	/*session*/
	public static final long SESSION_SHUTDOWN_TIMEOUT_MILLSEC = 2 * 1000; // extractor + transmitter will double timeout
	public static final long SESSION_THREAD_BARRIER_TIMEOUT_MILLSEC = 60 * 1000L;
	/*session end*/

	/*data and configuration file path*/
	public static final String BASE_DIR = System.getProperty("user.dir");
	/*data and configuration file path end*/

	/*cat*/
	public static final Pair<String, String> CONTAINER_REQUEST_CALL_FEQ = Pair.of(
		"Ptubes.Container",
		"RequestCall"
	);
	public static final Pair<String, String> ACCESS_LAYER_SUB_CALL_FEQ = Pair.of(
		"Ptubes.AccessLayer",
		"SubCall"
	);
	public static final Pair<String, String> ACCESS_LAYER_GET_CALL_FEQ = Pair.of(
		"Ptubes.AccessLayer",
		"BinlogGetCall"
	);

	// reader task error
	public static final String CAT_TASK_UNKNOWN_ERROR = "TaskError";
	public static final String CAT_READER_TASK_LOAD_CONFIG_ERROR = "LoadConfigError";
	public static final String CAT_READER_TASK_SETUP_ERROR = "SetupTaskError";
	public static final String CAT_READER_TASK_DUMP_POINT_CHANGE_ERROR = "AdjustDumpPointError";
	public static final String CAT_READER_TASK_STORAGE_MODE_CHANGE_ERROR = "SwitchStorageModeError";
	public static final String CAT_READER_TASK_STORAGE_FILE_RETENTION_CONFIG_CHANGE_ERROR = "UpdateStorageFileRetentionConfigError";
	public static final String CAT_READER_TASK_RDS_CHANGE_ERROR = "UpdateRdsConfigError";
	public static final String CAT_READER_TASK_SUBS_CHANGE_ERROR = "UpdateSubsConfigError";
	public static final String CAT_READER_TASK_BASIC_CHANGE_ERROR = "UpdateBasicConfigError";
	public static final String CAT_READER_TASK_CLEAN_EXPIRED_DATA_ERROR = "CleanExpiredDataError";
	public static final String CAT_READER_TASK_COMPRESS_DATA_ERROR = "CompressDataError";
	public static final String CAT_READER_TASK_UPDATE_CDC_CPKT_ERROR = "UpdateCDCCheckpointError";

	// session error
	public static final String CAT_SESSION_UNKNOWN_ERROR = "UnknownSessionError";
	public static final String CAT_SESSION_SUB_CONFIG_ERROR = "SubConfigError";
	public static final String CAT_SESSION_READ_EVENT_ERROR = "ReadEventError";
	public static final String CAT_SESSION_ENCODING_ERROR = "EncodingError";
	public static final String CAT_SESSION_INTERRUPT_ERROR = "SessionInterruptError";
	public static final String CAT_SESSION_THREAD_SYNC_ERROR = "SessionThreadSyncError";
	public static final String CAT_SESSION_READ_CHANNEL_OPEN_ERROR = "OpenrReadChannalError";
	public static final String CAT_SESSION_LESS_THAN_BUFFER_RANGE_ERROR = "SubCpLessThanStorageRange";
	public static final String CAT_SESSION_GREATER_THAN_BUFFER_RANGE_ERROR = "SubCpGreaterThanStorageRange";
	public static final String CAT_SESSION_SHUTDOWN_IN_SYNC_INTERRUPT_ERROR = "ShutdownSessionInSyncInterrupt";
	public static final String CAT_SESSION_SHUTDOWN_TIMEOUT_ERROR = "ShutdownSessionTimeout";
	// replication error
	public static final String CAT_PRODUCER_REPLICATOR_ERROR = "StartReplicatorError";
	public static final String CAT_PRODUCER_EVENT_SERIALIZER_ERROR = "SerializeEventError";
	public static final String CAT_PRODUCER_EVENTWRITER_ERROR = "WriteEventError";
	public static final String CAT_PRODUCER_REPLICATOR_LISTENER_ERROR = "ProcessEventError";
	public static final String CAT_PRODUCER_RECEIVE_ERROR_PACKET = "ReceiveErrorPacketFromDB";

	public static final String SESSION_EVENT_HEADER_EXTEND_IP = "sourceIP";
}
