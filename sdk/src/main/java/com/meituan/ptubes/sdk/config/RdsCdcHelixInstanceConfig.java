package com.meituan.ptubes.sdk.config;

import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.model.InstanceConfig;



public class RdsCdcHelixInstanceConfig extends InstanceConfig {

    private static final String STATE_TRANSITION_MAX_THREAD_KEY = "STATE_TRANSITION.maxThreads";

    public RdsCdcHelixInstanceConfig(String instanceId) {
        super(instanceId);
    }

    public int getStateTransitionMaxThread() {
        return _record.getIntField(
            STATE_TRANSITION_MAX_THREAD_KEY,
            HelixTaskExecutor.DEFAULT_PARALLEL_TASKS
        );
    }

    public void setStateTransitionMaxThread(int threadNum) {
        _record.setIntField(
            STATE_TRANSITION_MAX_THREAD_KEY,
            threadNum
        );
    }
}
