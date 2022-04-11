package com.meituan.ptubes.sdk.constants;



public class MonitorConstants {

    public static final String NETWORK_CAT_EVENT = "Rds.Cdc.Sdk.Network"; //Ptubes.Client.Network
    public static final String LATENCY_CAT_TRANSACTION = "Rds.Cdc.Sdk.Latency"; //Ptubes.Client.Latency
    public static final String FETCH_TIME_CAT_TRANSACTION = "Rds.Cdc.Sdk.FetchCost"; // fetch takes time
    public static final String FETCH_QUEUE_SIZE_CAT_TRANSACTION = "Rds.Cdc.Sdk.Fetch.QueueSize"; // The size of the fetch queue, corresponding to the 'time-consuming' item on the raptor, check before each write
    public static final String PARTITION_QUEUE_SIZE_CAT_TRANSACTION = "Rds.Cdc.Sdk.Partition.QueueSize."; // The size of each worker queue corresponds to the 'time-consuming' item on the raptor, which is checked before each write
    public static final String HEARTBEAT_LATENCY_CAT_TRANSACTION = "Rds.Cdc.Sdk.Heartbeat.Latency"; //Ptubes.Client.Heartbeat.Latency
    public static final String PARTITION_LATENCY_CAT_TRANSACTION = "Rds.Cdc.Sdk.Partition.Latency."; //Ptubes.Client.Partition.Latency.
    public static final String CONSUME_CAT_EVENT = "Rds.Cdc.Sdk.ConsumeEvent"; //Ptubes.Client.ConsumeEvent
    public static final String ACK_CAT_EVENT = "Rds.Cdc.Sdk.AckEvent"; // Time-consuming (delay) of adding in 1.5 ack
    public static final String FETCH_PULL_SIZE_CAT_EVENT = "Rds.Cdc.Sdk.ProduceEvent"; //Ptubes.Client.ProduceEvent
    public static final String ADD_PARTITION_CAT_EVENT = "Rds.Cdc.Sdk.Partition.Add"; //Ptubes.Client.Partition.Add
    public static final String DROP_PARTITION_CAT_EVENT = "Rds.Cdc.Sdk.Partition.Drop"; //Ptubes.Client.Partition.Drop
    public static final String BINLOG_TYPE_CAT_EVENT = "Rds.Cdc.Sdk.Event."; //Ptubes.Client.Event.
    public static final String CONTROL_CAT_TRANSACTION = "Rds.Cdc.Sdk.Control"; //Ptubes.Client.Control

    public static final String HANDLE_LION_CONFIG_CHANGE_FAIL = "Rds.Cdc.Sdk.HandleLionConfigChangeFail"; // Ptubes.Client.HandleLionConfigChangeFail
    public static final String CREATE_HELIX_CLUSTER_FAIL = "Rds.Cdc.Sdk.CreateHelixClusterFail."; // Ptubes.Client.CreateHelixClusterFail.
    public static final String HELIX_CLUSTER_NOT_READY = "Rds.Cdc.Sdk.HelixClusterNotReady."; // Ptubes.Client.HelixClusterNotReady.
    public static final String ABSTRACT_ACTOR_QUIT = "Rds.Cdc.Sdk.AbstractActorQuit."; // Ptubes.Client.AbstractActorQuit.
    public static final String ABSTRACT_ACTOR_QUIT_EXPECTED = "Rds.Cdc.Sdk.AbstractActorExpectedQuit."; // added in 1.5
    public static final String IGNORE_UPDATE_PARTITION_NUMBER = "Rds.Cdc.Sdk.IgnoreUpdatePartitionNumber."; // Ptubes.Client.IgnoreUpdatePartitionNumber.
    public static final String CONTROLLER_UPDATE_PARTITION_NUMBER_FAIL = "Rds.Cdc.Sdk.ControllerUpdatePartitionNumberFail."; // Ptubes.Client.ControllerUpdatePartitionNumberFail.

    public static final String CAT_CATEGORY_SUBSCRIBE_FAILED = "Rds.Cdc.Sdk.SubscribeFailed."; // Ptubes.Client.SubscribeFailed.
    public static final String CAT_CATEGORY_JSON_DER_ERR = "Rds.Cdc.Sdk.JsonDerErr"; // Ptubes.Client.JsonDerErr
    public static final String CAT_CATEGORY_READER_UNAVAILABLE = "Rds.Cdc.Sdk.ReaderUnavailable."; // Ptubes.Client.ReaderUnavailable.
    public static final String CAT_CATEGORY_READER_NO_CANDIDATE_READER = "Rds.Cdc.Sdk.NoCandidateReader."; // Ptubes.Client.NoCandidateReader.
    public static final String CAT_CATEGORY_READER_THROW_EVENT = "Rds.Cdc.Sdk.ThrowEvent."; // Ptubes.Client.ThrowEvent.

    public static final String HEARTBEAT_COLUMN_NAME = "utime";
}
