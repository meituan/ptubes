package com.meituan.ptubes.reader.monitor.vo;

public class ContainerInfo {

    private int numPeers;
    private long freqConnectionIdleTimeouts;
    private long inboundBytes;
    private long outboundBytes;

    public ContainerInfo() {
    }

    public ContainerInfo(
        int numPeers,
        long freqConnectionIdleTimeouts,
        long inboundBytes,
        long outboundBytes
    ) {
        this.numPeers = numPeers;
        this.freqConnectionIdleTimeouts = freqConnectionIdleTimeouts;
        this.inboundBytes = inboundBytes;
        this.outboundBytes = outboundBytes;
    }

    public int getNumPeers() {
        return numPeers;
    }

    public void setNumPeers(int numPeers) {
        this.numPeers = numPeers;
    }

    public long getFreqConnectionIdleTimeouts() {
        return freqConnectionIdleTimeouts;
    }

    public void setFreqConnectionIdleTimeouts(long freqConnectionIdleTimeouts) {
        this.freqConnectionIdleTimeouts = freqConnectionIdleTimeouts;
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
