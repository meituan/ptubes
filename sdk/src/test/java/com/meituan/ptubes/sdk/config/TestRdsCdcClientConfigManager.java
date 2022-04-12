package com.meituan.ptubes.sdk.config;

import com.meituan.ptubes.sdk.config.notification.SimpleLocalFileConfigChangeNotifier;
import org.junit.Before;
import org.junit.Test;



public class TestRdsCdcClientConfigManager {

    private String taskName;

    @Before
    public void setUp() {
        taskName = "clientDemo";
    }

    @Test
    public void test()  {
        RdsCdcClientConfigManager rdsCdcClientConfigManager = new RdsCdcClientConfigManager(
                taskName, new SimpleLocalFileConfigChangeNotifier(taskName));
        rdsCdcClientConfigManager.init();
    }

}
