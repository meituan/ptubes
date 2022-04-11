package com.meituan.ptubes.reader.container.config;

import com.meituan.ptubes.reader.container.common.config.ConfigService;
import com.meituan.ptubes.reader.container.common.config.netty.ServerConfig;

public class ServerConfigBuilder {

    public static final ServerConfigBuilder INSTANCE = new ServerConfigBuilder();

    public ServerConfig build(ConfigService configService) {
        int dataPort = Integer.valueOf(configService.getConfig(ConfigKeyConstants.ServerConfiguration.DATA_SERVER_PORT.getKey()));
        int monitorPort = Integer.valueOf(configService.getConfig(ConfigKeyConstants.ServerConfiguration.MONITOR_SERVER_PORT.getKey()));
        return new ServerConfig(dataPort, monitorPort, 1024, 1024, 4 * 1024, 8 * 1024 * 1024, 10 * 1024, 10 * 1024);
    }

}
