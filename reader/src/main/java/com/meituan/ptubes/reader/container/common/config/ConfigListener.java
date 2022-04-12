package com.meituan.ptubes.reader.container.common.config;

public interface ConfigListener {

    void onStart(ConfigService configService);

    void configChanged(ConfigEvent event);

    void onStop(ConfigService configService);

}
