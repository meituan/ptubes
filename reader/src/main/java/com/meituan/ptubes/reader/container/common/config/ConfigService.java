package com.meituan.ptubes.reader.container.common.config;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Load the service configuration
 */
public interface ConfigService {

    String getConfig(String key);

    /**
     * register listeners for config changing callback.
     * @param executor if executor is null, ConfigEvent should be handled by a global executor;
     *                 otherwise, ConfigEvent will be handled by the specific executor. More than one listener could
     *                 using the same executor if you want.
     * @param listener
     */
    void registerListener(ExecutorService executor, ConfigListener listener);

    void registerListeners(ExecutorService executor, List<ConfigListener> listeners);

    void removeListener(ConfigListener listener);

    void removeListeners(List<ConfigListener> listeners);

    void initialize();

    void destroy();
}
