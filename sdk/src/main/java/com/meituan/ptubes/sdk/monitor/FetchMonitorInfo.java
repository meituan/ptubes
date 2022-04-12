package com.meituan.ptubes.sdk.monitor;

import com.meituan.ptubes.sdk.model.ServerInfo;

public class FetchMonitorInfo {
    private long inboundBytes;
    private long outboundBytes;

    private ServerInfo targetServerInfo;

    public ServerInfo getTargetServerInfo() {
        return targetServerInfo;
    }

    public void setTargetServerInfo(ServerInfo targetServerInfo) {
        this.targetServerInfo = targetServerInfo;
    }

    public long getInboundBytes() {
        return inboundBytes;
    }

    public void setInboundBytes(long inboundBytes) {
        this.inboundBytes = inboundBytes;
    }

    public long getOutboundBytes() {
        return outboundBytes;
    }

    public void setOutboundBytes(long outboundBytes) {
        this.outboundBytes = outboundBytes;
    }
}
