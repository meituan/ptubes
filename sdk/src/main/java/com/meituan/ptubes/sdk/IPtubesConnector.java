package com.meituan.ptubes.sdk;

import com.meituan.ptubes.common.exception.RdsCdcRuntimeException;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.sdk.monitor.ConnectorMonitorInfo;

public interface IPtubesConnector<C extends BuffaloCheckpoint> {

    void registerEventListener(IRdsCdcEventListener eventListener) throws RdsCdcRuntimeException;

    void startup() throws Exception;

    void shutdown();

    void suspend() throws Exception;

    void resume() throws Exception;

    ConnectorStatus getStatus();

    ConnectorMonitorInfo<C> getConnectorMonitorInfo();

    /**
     * ack for asynchronously stream data
     */
    AckStatus ack(String idForAck);

    Logger getLog();
}
