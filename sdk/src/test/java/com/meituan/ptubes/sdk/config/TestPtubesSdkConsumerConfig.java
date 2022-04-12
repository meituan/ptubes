package com.meituan.ptubes.sdk.config;

import com.meituan.ptubes.common.utils.JacksonUtil;
import org.junit.Test;

public class TestPtubesSdkConsumerConfig {
    String consumeConfigLionStringMysqlOld = "{\"failureMode\":\"RETRY\",\"retryTimes\":-1,\"qpsLimit\":100,\"checkpointSyncIntervalMs\":120000,\"consumptionMode\":\"BATCH\",\"batchConsumeSize\":50,\"batchConsumeTimeoutMs\":100,\"workerTimeoutMs\":6000}";

    @Test
    public void testDeserializeMysqlOldConfig() {
        PtubesSdkConsumerConfig config = JacksonUtil.fromJson(
                consumeConfigLionStringMysqlOld,
                PtubesSdkConsumerConfig.class
        );
        System.out.println(config);
        assert null != config;
        assert null == config.getAckType();
    }
}
