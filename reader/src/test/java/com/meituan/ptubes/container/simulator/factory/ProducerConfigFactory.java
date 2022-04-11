package com.meituan.ptubes.container.simulator.factory;

import com.meituan.ptubes.container.simulator.TestConstant;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerBaseConfig;
import com.meituan.ptubes.reader.container.common.config.producer.RdsConfig;
import java.util.HashMap;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig;

public class ProducerConfigFactory {
    public static ProducerConfig newProducerConfig() {
        return new ProducerConfig(TestConstant.READER_TASK_NAME, newProducerBaseConfig(), new HashMap<>(), newRdsConfig());
    }

    public static ProducerBaseConfig newProducerBaseConfig() {
        return new ProducerBaseConfig();
    }

    public static RdsConfig newRdsConfig() {
        RdsConfig rdsConfig = new RdsConfig();
        rdsConfig.setPassword(TestConstant.READER_TASK_RDS_PASSWORD);
        rdsConfig.setUserName(TestConstant.READER_TASK_RDS_USER_NAME);
        return rdsConfig;
    }
}
