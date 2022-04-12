package com.meituan.ptubes.sdk;



public enum RdsCdcEventStatus {
    SUCCESS(0, "SUCCESS"),

    FAILURE(1, "FAILURE"),

    SKIP(2, "SKIP");


    private int status;
    private String name;

    private RdsCdcEventStatus(int status, String name) {
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
