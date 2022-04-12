package com.meituan.ptubes.reader.container.network.connections;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.network.request.GetRequest;
import java.util.concurrent.TimeUnit;
import com.meituan.ptubes.reader.container.common.AbstractLifeCycle;
import com.meituan.ptubes.reader.container.network.request.sub.SubRequest;

public abstract class AbstractClientSession extends AbstractLifeCycle {

    protected final String clientId; // writeTaskName + shortChannelId (don't bind ip, reconnect can't recognize it)
    protected final String readerTaskName;
    protected final SourceType sourceType;

    public AbstractClientSession(String clientId, String readerTaskName, SourceType sourceType) {
        this.clientId = clientId;
        this.readerTaskName = readerTaskName;
        this.sourceType = sourceType;
    }

    public String getClientId() {
        return clientId;
    }

    public String getReaderTaskName() {
        return readerTaskName;
    }

    public SourceType getSourceType() {
        return sourceType;
    }

    public abstract void resetAsynchronously(SubRequest subRequest, AsyncOpResListener reSubListener);

    public abstract void startup();

    public abstract AbstractClientSession closeAsync();
    // Separate use for closeAsync asynchronously
    public abstract void waitForClose() throws InterruptedException;
    public abstract boolean waitForCloseWithTimeout(long timeout, TimeUnit timeUnit) throws InterruptedException;

    public abstract void closeInSync() throws InterruptedException;
    public abstract boolean closeInSyncWithTimeout(long timeout, TimeUnit timeUnit) throws InterruptedException;

    public abstract boolean offerGetRequest(GetRequest getRequest);

}
