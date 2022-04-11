package com.meituan.ptubes.reader.producer.mysqlreplicator.component;

import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.WorkHandler;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.BinlogData;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public class BinlogWriter implements WorkHandler<BinlogData>, TimeoutHandler, LifecycleAware {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogWriter.class);

    private final BinlogPipeline parent;

    public BinlogWriter(BinlogPipeline parent) {
        this.parent = parent;
    }

    @Override public void onEvent(BinlogData event) throws Exception {
        if (parent.isShutdownRequested()) {
            return;
        }

        if (event.isSkipable() == false) {
            try {
                ChangeEntry changeEntry = event.getChangeEntry();
                int retryTime = 5;
                while (!parent.isShutdownRequested() && retryTime > 0) {
                    try {
                        writeEvent(changeEntry);
                        break;
                    } catch (Throwable t) {
                        retryTime--;
                        if (retryTime == 0) {
                            LOG.error("Got exception when write transaction, will drop it, binlogInfo: {}", changeEntry.getBinlogInfo().toString(), t);
                        } else {
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                    }
                }
            } catch (Throwable te) {
                LOG.error("{} persist row data error, skip row data at txn = {}, row = {}", parent.eventProducer.getReaderTaskName(),
                    event.getTxnContext().toString(), event.getRowContext().toString(), te);

            }
        } else {
            return;
        }
    }

    private void writeEvent(ChangeEntry changeEntry) throws PtubesException {
        try {
            parent.writeChannel.append(changeEntry);
            boolean maxBinlogInfoSaved = parent.maxBinlogInfoReaderWriter.saveMaxBinlogInfo(changeEntry.getMaxBinlogInfo());
            if (maxBinlogInfoSaved) {
                parent.eventProducer.pushForwardCheckpoint(changeEntry.getBinlogInfo());
            }
        } catch (Exception e) {
            LOG.error("Got exception while writting events to the buffer", e);
            throw new PtubesException(e);
        }
    }

    @Override
    public void onStart() {
        LOG.info("{} Binlog Writer start!", parent.eventProducer.getReaderTaskName());
    }

    @Override
    public void onShutdown() {
        LOG.info("{} Binlog Writer shutdown!", parent.eventProducer.getReaderTaskName());
    }

    @Override
    public void onTimeout(long sequence) throws Exception {
        LOG.info("{} Binlog Writer waitfor new data timeout!", parent.eventProducer.getReaderTaskName());
    }
}
