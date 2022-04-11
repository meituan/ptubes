package com.meituan.ptubes.reader.container.config;

import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.reader.container.common.config.ConfigService;
import com.meituan.ptubes.reader.container.common.config.producer.TableConfig;
import com.meituan.ptubes.reader.container.common.constants.ProducerConstants;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import com.meituan.ptubes.reader.container.common.config.ConfigBuilder;
import org.apache.commons.lang3.StringUtils;

public class SubscriptionConfigBuilder implements ConfigBuilder<Map<String, TableConfig>> {

    public static SubscriptionConfigBuilder BUILDER = new SubscriptionConfigBuilder();

    @Nonnull
    @Override public Map<String, TableConfig> build(ConfigService configService, String readerTaskName) {
        String subsString = configService.getConfig(ConfigKeyConstants.TaskConfiguration.genConfigKey(readerTaskName, ConfigKeyConstants.TaskConfiguration.MYSQL_SUBSCRIPTION));
        Map<String, TableConfig> tableConfigMap;
        if (StringUtils.isNotBlank(subsString)) {
            tableConfigMap = new HashMap<>();
            Map<String, String> subsMap = JSONUtil.jsonToMap(subsString, String.class, String.class);
            for (Map.Entry<String, String> subsMapEntry : subsMap.entrySet()) {
                String fullTableName = subsMapEntry.getKey();
                String partitionKey = subsMapEntry.getValue();
                String[] databaseAndTable = fullTableName.split("\\.");
                tableConfigMap.put(fullTableName, new TableConfig(databaseAndTable[0], databaseAndTable[1], partitionKey));
            }
            tableConfigMap.put(ProducerConstants.HEARTBEAT_DB_TABLE_NAME, new TableConfig(ProducerConstants.HEARTBEAT_SCHEMA, ProducerConstants.HEARTBEAT_TABLE, ProducerConstants.HEARTBEAT_TABLE_ID_COL_NAME));
        } else {
            // subscribe all tables
            tableConfigMap = Collections.EMPTY_MAP;
        }
        return tableConfigMap;
    }
}
