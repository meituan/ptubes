package com.meituan.ptubes.reader.monitor.vo;

import java.util.Map;

public class ClientSessionInfo {

    private String writerTaskName;
    private String remoteHost;
    private int port;

    private long inboundBytes;
    private long outboundBytes;

    private long freqSubCalls;
    private long latencySubCall;
    private long freqErrorSubs;
    private long lastSubTimestamp;
    private long connectTimestamp; // First subscription time

    private long freqBinlogGetCalls;
    private long latencyBinlogGetCall;
    private long freqErrorBinlogGets;
    private long lastBinlogGetTimestamp;

    private Map<String, Object> latestBinlogGetBinlogInfo;

    public ClientSessionInfo(
        String writerTaskName,
        String remoteHost,
        int port,
        long inboundBytes,
        long outboundBytes,
        long freqSubCalls,
        long latencySubCall,
        long freqErrorSubs,
        long lastSubTimestamp,
        long connectTimestamp,
        long freqBinlogGetCalls,
        long latencyBinlogGetCall,
        long freqErrorBinlogGets,
        long lastBinlogGetTimestamp,
        Map<String, Object> latestBinlogGetBinlogInfo
    ) {
        this.writerTaskName = writerTaskName;
        this.remoteHost = remoteHost;
        this.port = port;
        this.inboundBytes = inboundBytes;
        this.outboundBytes = outboundBytes;
        this.freqSubCalls = freqSubCalls;
        this.latencySubCall = latencySubCall;
        this.freqErrorSubs = freqErrorSubs;
        this.lastSubTimestamp = lastSubTimestamp;
        this.connectTimestamp = connectTimestamp;
        this.freqBinlogGetCalls = freqBinlogGetCalls;
        this.latencyBinlogGetCall = latencyBinlogGetCall;
        this.freqErrorBinlogGets = freqErrorBinlogGets;
        this.lastBinlogGetTimestamp = lastBinlogGetTimestamp;
        this.latestBinlogGetBinlogInfo = latestBinlogGetBinlogInfo;
    }

    public String getWriterTaskName() {
        return writerTaskName;
    }

    public void setWriterTaskName(String writerTaskName) {
        this.writerTaskName = writerTaskName;
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public void setRemoteHost(String remoteHost) {
        this.remoteHost = remoteHost;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

    public long getFreqSubCalls() {
        return freqSubCalls;
    }

    public void setFreqSubCalls(long freqSubCalls) {
        this.freqSubCalls = freqSubCalls;
    }

    public long getLatencySubCall() {
        return latencySubCall;
    }

    public void setLatencySubCall(long latencySubCall) {
        this.latencySubCall = latencySubCall;
    }

    public long getFreqErrorSubs() {
        return freqErrorSubs;
    }

    public void setFreqErrorSubs(long freqErrorSubs) {
        this.freqErrorSubs = freqErrorSubs;
    }

    public long getLastSubTimestamp() {
        return lastSubTimestamp;
    }

    public void setLastSubTimestamp(long lastSubTimestamp) {
        this.lastSubTimestamp = lastSubTimestamp;
    }

    public long getConnectTimestamp() {
        return connectTimestamp;
    }

    public void setConnectTimestamp(long connectTimestamp) {
        this.connectTimestamp = connectTimestamp;
    }

    public long getFreqBinlogGetCalls() {
        return freqBinlogGetCalls;
    }

    public void setFreqBinlogGetCalls(long freqBinlogGetCalls) {
        this.freqBinlogGetCalls = freqBinlogGetCalls;
    }

    public long getLatencyBinlogGetCall() {
        return latencyBinlogGetCall;
    }

    public void setLatencyBinlogGetCall(long latencyBinlogGetCall) {
        this.latencyBinlogGetCall = latencyBinlogGetCall;
    }

    public long getFreqErrorBinlogGets() {
        return freqErrorBinlogGets;
    }

    public void setFreqErrorBinlogGets(long freqErrorBinlogGets) {
        this.freqErrorBinlogGets = freqErrorBinlogGets;
    }

    public long getLastBinlogGetTimestamp() {
        return lastBinlogGetTimestamp;
    }

    public void setLastBinlogGetTimestamp(long lastBinlogGetTimestamp) {
        this.lastBinlogGetTimestamp = lastBinlogGetTimestamp;
    }

    public Map<String, Object> getLatestBinlogGetBinlogInfo() {
        return latestBinlogGetBinlogInfo;
    }

    public void setLatestBinlogGetBinlogInfo(Map<String, Object> latestBinlogGetBinlogInfo) {
        this.latestBinlogGetBinlogInfo = latestBinlogGetBinlogInfo;
    }
}
