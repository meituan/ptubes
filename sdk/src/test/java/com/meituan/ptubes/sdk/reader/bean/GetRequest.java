package com.meituan.ptubes.sdk.reader.bean;

public class GetRequest {

    private int eventSize;
    private int eventNum;
    private long timeout;

    public GetRequest(
        int eventSize,
        int eventNum,
        long timeout
    ) {
        this.eventSize = eventSize;
        this.eventNum = eventNum;
        this.timeout = timeout;
    }

    public int getEventSize() {
        return eventSize;
    }

    public void setEventSize(int eventSize) {
        this.eventSize = eventSize;
    }

    public int getEventNum() {
        return eventNum;
    }

    public void setEventNum(int eventNum) {
        this.eventNum = eventNum;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
