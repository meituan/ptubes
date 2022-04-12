package com.meituan.ptubes.reader.producer.mysqlreplicator.common;

import com.lmax.disruptor.ExceptionHandler;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public class PipelineExceptionHandler implements ExceptionHandler<BinlogData> {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineExceptionHandler.class);

    @Override public void handleEventException(Throwable ex, long sequence, BinlogData event) {
        LOG.error("exception escaped!!", ex);
    }

    @Override public void handleOnStartException(Throwable ex) {
        LOG.error("startup error", ex);
    }

    @Override public void handleOnShutdownException(Throwable ex) {
        LOG.error("shutdown error", ex);
    }
}
