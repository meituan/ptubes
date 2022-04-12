package com.meituan.ptubes.reader.container.config;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.ConfigService;
import com.meituan.ptubes.reader.container.common.config.producer.RdsConfig;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.Query;
import javax.annotation.Nullable;
import com.meituan.ptubes.reader.container.common.config.ConfigBuilder;

public class RdsConfigBuilder implements ConfigBuilder<RdsConfig> {

    public static final Logger LOG = LoggerFactory.getLogger(ConfigBuilder.class);
    public static RdsConfigBuilder BUILDER = new RdsConfigBuilder();

    @Nullable
    @Override public RdsConfig build(ConfigService configService, String readerTaskName) {
        try {
            String host = configService.getConfig(ConfigKeyConstants.TaskConfiguration.genConfigKey(readerTaskName, ConfigKeyConstants.TaskConfiguration.MYSQL_HOST));
            int port = Integer.valueOf(configService.getConfig(ConfigKeyConstants.TaskConfiguration.genConfigKey(readerTaskName, ConfigKeyConstants.TaskConfiguration.MYSQL_PORT)));
            String user = configService.getConfig(ConfigKeyConstants.TaskConfiguration.genConfigKey(readerTaskName, ConfigKeyConstants.TaskConfiguration.MYSQL_USER));
            String passwd = configService.getConfig(ConfigKeyConstants.TaskConfiguration.genConfigKey(readerTaskName, ConfigKeyConstants.TaskConfiguration.MYSQL_PASSWD));
            Query query = new Query(Query.getDefaultTransport(host, port, user, passwd));
            long serverId = -1;
            try {
                serverId = Long.valueOf(query.get("select @@server_id").get(0));
            } finally {
                query.close();
            }
            return new RdsConfig(host, port, user, passwd, serverId);
        } catch (Exception e) {
            LOG.error("Rds Config Builder error", e);
            return null;
        }
    }
}
