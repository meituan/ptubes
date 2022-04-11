package com.meituan.ptubes.sdk.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.common.utils.JacksonUtil;
import org.junit.Test;

public class TestPtubesSdkSubscriptionConfig {
    public static  String subscriptionConfigLionStringMysql = "{\"buffaloCheckpoint\":{\"checkpointMode\":\"LATEST\",\"versionTs\":1626158433657,\"uuid\":\"00000000-0000-0000-0000-000000000000\",\"transactionId\":-1,\"eventIndex\":-1,\"serverId\":-1,\"binlogFile\":-1,\"binlogOffset\":-1,\"timestamp\":-1},\"sourceType\":\"MYSQL\",\"readerAppkey\":\"com.sankuai.dts.inf.readertest01\",\"routerMode\":\"ZOOKEEPER\",\"routerAddress\":\"127.0.0.1\",\"fetchBatchSize\":1000,\"fetchTimeoutMs\":50,\"fetchMaxByteSize\":1000000,\"needDDL\":false,\"needEndTransaction\":false,\"partitionNum\":71,\"serviceGroupInfo\":{\"taskName\":\"test_db_to_mq_common\",\"serviceGroupName\":\"dbustest2-5\",\"databaseInfoSet\":[{\"databaseName\":\"test_wxxxx\",\"tableNames\":[\"Airport\",\"test_table1\",\"test_table2\",\"test_table3\",\"test_table\"]}]}}";
    public static  String subscriptionConfigLionStringMysqlOld = "{\"buffaloCheckpoint\":{\"checkpointMode\":\"LATEST\",\"versionTs\":1628146746082,\"uuid\":\"00000000-0000-0000-0000-000000000000\",\"transactionId\":-1,\"eventIndex\":-1,\"serverId\":-1,\"binlogFile\":-1,\"binlogOffset\":-1,\"timestamp\":-1},\"readerAppkey\":\"com.sankuai.dts.inf.testreader\",\"routerMode\":\"ZOOKEEPER\",\"routerAddress\":\"127.0.0.3\",\"fetchBatchSize\":1000,\"fetchTimeoutMs\":50,\"fetchMaxByteSize\":1000000,\"needDDL\":false,\"needEndTransaction\":false,\"partitionNum\":3,\"serviceGroupInfo\":{\"taskName\":\"AutotestMixMafka\",\"serviceGroupName\":\"dbustest1-2\",\"databaseInfoSet\":[{\"databaseName\":\"autotest_source\",\"tableNames\":[\"_shadow_test_table_2_\",\"test_table_2\"]}]}}";

    /**
     * The lion data (MYSQL) with sourceType can be parsed by the new version client
     */
    @Test
    public void testDeserializeMysqlCheckpoint() {
        System.out.println(subscriptionConfigLionStringMysql);
        PtubesSdkSubscriptionConfig config = JacksonUtil.fromJson(
                subscriptionConfigLionStringMysql,
                PtubesSdkSubscriptionConfig.class
        );
        System.out.println(config);
        System.out.println(config.getBuffaloCheckpoint());
        assert config.getSourceType() == RdsCdcSourceType.MYSQL;
    }

    /**
     * The lion data (MYSQL) with sourceType cannot be parsed by the old version client
     */
    @Test
    public void testDeserializeMysqlOldCheckpoint() {
        System.out.println(subscriptionConfigLionStringMysqlOld);
        RdsCdcClientSubscriptionConfigOld config = JacksonUtil.fromJson(
                subscriptionConfigLionStringMysql,
                RdsCdcClientSubscriptionConfigOld.class
        );
        System.out.println(config);
        assert null == config;
    }

    /**
     * The lion data (MYSQL) with missing sourceType can be parsed by the new version of the client (the tasks of the lower version run to the cluster of the higher version, OK)
     */
    @Test
    public void testDeserializeMysqlOldCheckpointInNewVersion() {
        System.out.println(subscriptionConfigLionStringMysqlOld);

        PtubesSdkSubscriptionConfig config = JacksonUtil.fromJson(
                subscriptionConfigLionStringMysqlOld.toString(),
                PtubesSdkSubscriptionConfig.class
        );
        System.out.println(config);
        System.out.println(config.getBuffaloCheckpoint());
        System.out.println(config.getSourceType());
        assert ((MysqlCheckpoint)config.getBuffaloCheckpoint()).getVersionTs() == 1628146746082L;
        assert config.getSourceType() == null;
    }

    /**
     * Deprecated Validate yourself to complete missing fields for json
     * @throws Exception
     */
    // @Test
    @Deprecated
    public void testJsonParserAddNodeV2() throws Exception {
        System.out.println(subscriptionConfigLionStringMysqlOld);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(subscriptionConfigLionStringMysqlOld);
        assert null == node.get("sourceType");
        if (null == node.get("sourceType")) {
            ((ObjectNode) node).put("sourceType", "MYSQL");
        }
        assert null != node.get("sourceType");
        assert node.get("sourceType").textValue().equals("MYSQL");


        PtubesSdkSubscriptionConfig config = JacksonUtil.fromJson(
                node.toString(),
                PtubesSdkSubscriptionConfig.class
        );
        System.out.println(config);
        System.out.println(config.getBuffaloCheckpoint());
        assert ((MysqlCheckpoint)config.getBuffaloCheckpoint()).getVersionTs() == 1626158433657L;
        assert ((MysqlCheckpoint)config.getBuffaloCheckpoint()).getCheckpointMode() == BuffaloCheckpoint.CheckpointMode.NORMAL;
    }
}
