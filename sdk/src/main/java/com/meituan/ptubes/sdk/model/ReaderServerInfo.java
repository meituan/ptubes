package com.meituan.ptubes.sdk.model;

public class ReaderServerInfo {
    private String ip;
    private int port;

    public ReaderServerInfo() {
    }

    public ReaderServerInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public ReaderServerInfo setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ReaderServerInfo setPort(int port) {
        this.port = port;
        return this;
    }
}
