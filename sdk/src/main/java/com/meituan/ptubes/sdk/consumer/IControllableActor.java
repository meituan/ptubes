package com.meituan.ptubes.sdk.consumer;



/**
 * An interface for sending control messages to an actor.
 */
public interface IControllableActor {
    void pause();

    void resume();

    void shutdown();
}
