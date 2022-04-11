package com.meituan.ptubes.storage;

import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.thread.PtubesThreadBase;
import com.meituan.ptubes.reader.storage.channel.DefaultReadChannel;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;

public class ReadTask extends PtubesThreadBase {

    private volatile DefaultReadChannel readChannel;
    private AtomicInteger dataEventCount = new AtomicInteger(0);
    private AtomicInteger commitEventCount = new AtomicInteger(0);
    private AtomicInteger otherEventCount = new AtomicInteger(0);
    private volatile boolean noMoreEvent = false;
    private MySQLBinlogInfo startBinlogInfo;

    public ReadTask(
        String name,
        DefaultReadChannel readChannel,
        MySQLBinlogInfo startBinlogInfo
    ) {
        super(name);
        this.readChannel = readChannel;
        this.startBinlogInfo = startBinlogInfo;
    }

    public int getDataEventCount() {
        return dataEventCount.get();
    }

    public int getCommitEventCount() {
        return commitEventCount.get();
    }

    public int getOtherEventCount() {
        return otherEventCount.get();
    }

    public boolean isNoMoreEvent() {
        return noMoreEvent;
    }

    public void switchStorageMode(StorageConstant.StorageMode storageMode)
        throws InterruptedException, LessThanStorageRangeException {
        readChannel.switchStorageMode(storageMode);
        if (readChannel.getStorageMode() != storageMode) {
            throw new AssertionError("SwitchStorageMode error");
        }
    }

    @Override
    public void run() {
        log.info("ReadTask Thread start");
        while (!isShutdownRequested()) {
            if (isPauseRequested()) {
                log.info("Pause requested for ReadTask. Pausing !!");
                signalPause();
                log.info("Pausing. Waiting for resume command");
                try {
                    awaitUnPauseRequest();
                } catch (InterruptedException e) {
                    log.info(
                        "ReadTask {} Interrupted !",
                        getName(),
                        e
                    );
                }
                log.info("Resuming ReadTask !!");
                signalResumed();
                log.info("ReadTask resumed !!");
            }

            try {
                PtubesEvent event = this.readChannel.next();
                if (EventType.isErrorEvent(event.getEventType())) {
                    if (EventType.NO_MORE_EVENT == event.getEventType()) {
                        this.noMoreEvent = true;
                    }
                    continue;
                }
                if (EventType.COMMIT == event.getEventType()) {
                    if (isBinlogInfoRight(
                        (MySQLBinlogInfo) event.getBinlogInfo(),
                        true
                    )) {
                        commitEventCount.incrementAndGet();
                    } else {
                        otherEventCount.incrementAndGet();
                    }
                } else if (EventType.INSERT == event.getEventType() || EventType.UPDATE == event.getEventType() ||
                    EventType.DELETE == event.getEventType()) {
                    if (isBinlogInfoRight(
                        (MySQLBinlogInfo) event.getBinlogInfo(),
                        false
                    )) {
                        dataEventCount.incrementAndGet();
                    } else {
                        otherEventCount.incrementAndGet();
                    }
                } else {
                    otherEventCount.incrementAndGet();
                }
            } catch (IOException e) {
                log.error(
                    "Read next error",
                    e
                );
            }

            try {
                sleep(10);
            } catch (InterruptedException e) {
                log.info("Sleep InterruptedException");
            }
        }

        log.info("ReadTask Thread done");
        doShutdownNotify();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.readChannel.stop();
    }

    private boolean isBinlogInfoRight(
        MySQLBinlogInfo binlogInfo,
        boolean isCommit
    ) {
        long binlogOffset = isCommit ? startBinlogInfo.getBinlogOffset() + commitEventCount.get() + 1
            : startBinlogInfo.getBinlogOffset() + dataEventCount.get() + 1;
        long txnId = isCommit ? startBinlogInfo.getTxnId() + commitEventCount.get() + 1
            : startBinlogInfo.getTxnId() + dataEventCount.get() + 1;
        long eventIndex = isCommit ? 1 : 0;
        MySQLBinlogInfo expectBinlogInfo = new MySQLBinlogInfo(
            startBinlogInfo.getChangeId(),
            startBinlogInfo.getServerId(),
            startBinlogInfo.getBinlogId(),
            binlogOffset,
            startBinlogInfo.getUuid(),
            txnId,
            eventIndex,
            startBinlogInfo.getTimestamp()
        );
        return expectBinlogInfo.isEqualTo(
            binlogInfo,
            StorageConstant.IndexPolicy.BINLOG_OFFSET
        );

    }
}
