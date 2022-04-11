package com.meituan.ptubes.reader.storage.mem;

import java.util.concurrent.atomic.AtomicInteger;

public class PooledMemStorage {

    private final MemStorage memStorage;
    private final AtomicInteger refCnt;

    public PooledMemStorage(MemStorage memStorage) {
        this.memStorage = memStorage;
        this.refCnt = new AtomicInteger(1);
    }

    public MemStorage getMemStorage() {
        return memStorage;
    }

    public AtomicInteger getRefCnt() {
        return refCnt;
    }

    public int retain() {
        return refCnt.incrementAndGet();
    }

    public int release() {
        return refCnt.decrementAndGet();
    }
}
