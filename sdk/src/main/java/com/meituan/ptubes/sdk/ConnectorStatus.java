package com.meituan.ptubes.sdk;



public enum ConnectorStatus {

    INIT(0, "INIT"),

    RUNNING(1, "RUNNING"),

    SUSPENDED(2, "SUSPENDED"),

    CLOSED(3, "CLOESED");

    private int status;
    private String name;

    private ConnectorStatus(int status, String name) {
        this.status = status;
        this.name = name;
    }

    public int getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }
}
