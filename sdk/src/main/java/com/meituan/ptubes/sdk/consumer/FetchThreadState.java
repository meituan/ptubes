package com.meituan.ptubes.sdk.consumer;

import com.meituan.ptubes.sdk.protocol.RdsPacket;


public class FetchThreadState {

    private RdsPacket.RdsEnvelope rdsEnvelope;
    private StateId stateId;

    FetchThreadState() {
        this.setStateId(StateId.START);
    }

    public enum StateId {
        START,
        PICK_SERVER,
        CONNECT,
        CONNECT_SUCCESS,
        CONNECT_FAILURE,
        SUBSCRIBE,
        SUBSCRIBE_SUCCESS,
        SUBSCRIBE_FAILURE,
        FETCH_EVENTS,
        FETCH_EVENTS_SUCCESS,
        FETCH_EVENTS_FAILURE,
        OFFER_DISPATCH_EVENTS
    }

    public StateId getStateId() {
        return stateId;
    }

    public void setStateId(StateId stateId) {
        this.stateId = stateId;
    }

    public RdsPacket.RdsEnvelope getRdsEnvelope() {
        return rdsEnvelope;
    }

    public void setRdsEnvelope(RdsPacket.RdsEnvelope rdsEnvelope) {
        this.rdsEnvelope = rdsEnvelope;
    }
}
