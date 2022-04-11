package com.meituan.ptubes.sdk.consumer;



/**
 * An interface for sending messages to an actor.
 */
public interface IActorMessageQueue {
    /**
     * Add the message to the actor's message queue.
     *
     * @param message
     */
    public void enqueueMessage(Object message);
}
