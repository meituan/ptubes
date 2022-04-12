package com.meituan.ptubes.sdk.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.net.InetSocketAddress;
import java.util.List;



// Supplementary error message fields
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerInfo implements Comparable<ServerInfo> {
    private ServerStatus serverStatus;
    private InetSocketAddress address;
    private List<String> writerTaskList;
    private long inboundBytes;
    private long outboundBytes;
    private int loadFactor;

    public ServerStatus getServerStatus() {
        return serverStatus;
    }

    public void setServerStatus(ServerStatus serverStatus) {
        this.serverStatus = serverStatus;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public void setAddress(InetSocketAddress address) {
        this.address = address;
    }

    public List<String> getWriterTaskList() {
        return writerTaskList;
    }

    public void setWriterTaskList(List<String> writerTaskList) {
        this.writerTaskList = writerTaskList;
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

    public int getLoadFactor() {
        return loadFactor;
    }

    public void setLoadFactor(int loadFactor) {
        this.loadFactor = loadFactor;
    }

    @Override
    public int compareTo(ServerInfo other) {
        return this.loadFactor - other.getLoadFactor();
    }

    @Override
    public String toString() {
        return "ServerInfo{" +
                "serverStatus=" + serverStatus +
                ", address=" + address +
                ", writerTaskList=" + writerTaskList +
                ", inboundBytes=" + inboundBytes +
                ", outboundBytes=" + outboundBytes +
                ", loadFactor=" + loadFactor +
                '}';
    }

    public enum ServerStatus {
        UNKNOWN,
        ERROR,
        OFFLINE,
        UNAVAILABLE,
        AVAILABLE
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }

        if (! (obj instanceof ServerInfo)) {
            return false;
        }

        ServerInfo other = (ServerInfo) obj;

        return this.address.equals(other.getAddress());
    }

    @Override
    public int hashCode() {
        return this.address.hashCode();
    }
}
