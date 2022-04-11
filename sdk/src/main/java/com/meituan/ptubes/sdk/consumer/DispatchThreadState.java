package com.meituan.ptubes.sdk.consumer;



public class DispatchThreadState {

    private StateId stateId;

    public DispatchThreadState() {
        this.stateId = StateId.START;
    }

    public enum StateId {
        START,
        DISPATCH_EVENT
    }

    public StateId getStateId() {
        return stateId;
    }

    public void setStateId(StateId stateId) {
        this.stateId = stateId;
    }
}
