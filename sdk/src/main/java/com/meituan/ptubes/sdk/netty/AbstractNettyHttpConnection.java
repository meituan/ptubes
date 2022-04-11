package com.meituan.ptubes.sdk.netty;



public class AbstractNettyHttpConnection implements RdsCdcServerConnection {

    private State state;

    /** Connection state */
    enum State {
        INIT,
        CONNECTING,
        CONNECTED,
        CLOSING,
        CLOSED
    }

    public AbstractNettyHttpConnection() {
        this.state = State.INIT;
    }

    @Override
    public synchronized void close() {
        this.state = State.CLOSED;
    }

    public synchronized State getState() {
        return state;
    }

    public synchronized void setState(State state) {
        this.state = state;
    }

    @Override
    public String getRemoteHost() {
        return null;
    }

    @Override
    public String getRemoteService() {
        return null;
    }
}
