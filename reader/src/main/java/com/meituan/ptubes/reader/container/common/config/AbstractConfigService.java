package com.meituan.ptubes.reader.container.common.config;

import java.util.List;
import java.util.concurrent.ExecutorService;

public abstract class AbstractConfigService implements ConfigService {

    @Override public String getConfig(String key) {
        return null;
    }

    @Override public void registerListener(ExecutorService executor, ConfigListener listener) {

    }

    @Override public void registerListeners(ExecutorService executor, List<ConfigListener> listeners) {

    }

    @Override public void removeListener(ConfigListener listener) {

    }

    @Override public void removeListeners(List<ConfigListener> listeners) {

    }

    @Override public void initialize() {

    }

    @Override public void destroy() {

    }
}
