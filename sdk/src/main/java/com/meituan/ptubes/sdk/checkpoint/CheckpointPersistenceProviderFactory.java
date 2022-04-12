package com.meituan.ptubes.sdk.checkpoint;

import com.meituan.ptubes.common.exception.RdsCdcRuntimeException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.meituan.ptubes.sdk.config.PtubesSdkSubscriptionConfig;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;

public class CheckpointPersistenceProviderFactory {

    private static Map<String, ICheckpointPersistenceProvider> checkpointPersistenceProviderMap = new ConcurrentHashMap<>();

    public synchronized static void initPersistenceProvider(
        String taskName,
        String zkAddr,
        PtubesSdkSubscriptionConfig.RouterMode routerMode,
        RdsCdcSourceType sourceType
    ) throws Exception {
        if (checkpointPersistenceProviderMap.containsKey(taskName)) {
            return;
        }

        ICheckpointPersistenceProvider checkpointPersistenceProvider = null;
        switch (routerMode) {
            case ZOOKEEPER:
                checkpointPersistenceProvider = new ZKCheckpointPersistenceProvider(
                    taskName,
                    zkAddr,
                    sourceType
                );
                break;
            case ROUTER:
                break;
            default:
        }

        checkpointPersistenceProviderMap.put(
            taskName,
            checkpointPersistenceProvider
        );
    }

    public synchronized static void initPersistenceProvider(
        String taskName,
        ICheckpointPersistenceProvider checkpointPersistenceProvider
    ) {
        if (checkpointPersistenceProviderMap.containsKey(taskName)) {
            return;
        }

        checkpointPersistenceProviderMap.put(
            taskName,
            checkpointPersistenceProvider
        );
    }

    public synchronized static ICheckpointPersistenceProvider getInstance(String taskName) throws
        RdsCdcRuntimeException {
        if (!checkpointPersistenceProviderMap.containsKey(taskName)) {
            throw new RdsCdcRuntimeException(
                "The CheckpointPersistenceProvider instance " + taskName + " has not been initialized.");
        }

        return checkpointPersistenceProviderMap.get(taskName);
    }

    public synchronized static void shutdown(String taskName) {
        ICheckpointPersistenceProvider checkpointPersistenceProvider = checkpointPersistenceProviderMap.get(taskName);

        if (checkpointPersistenceProvider != null) {
            checkpointPersistenceProvider.close();
            checkpointPersistenceProviderMap.remove(taskName);
        }
    }
}
