package com.meituan.ptubes.sdk;

import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.sdk.config.RdsCdcClientConfigManager;

public class MysqlConnector extends PtubesConnector<MysqlCheckpoint> {
    public MysqlConnector(String taskName, RdsCdcClientConfigManager rdsCdcClientConfigManager) {
        super(taskName, rdsCdcClientConfigManager);
    }
}
