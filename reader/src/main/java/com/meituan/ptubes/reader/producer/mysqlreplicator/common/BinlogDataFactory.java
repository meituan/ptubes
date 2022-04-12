package com.meituan.ptubes.reader.producer.mysqlreplicator.common;

import com.lmax.disruptor.EventFactory;

public class BinlogDataFactory implements EventFactory<BinlogData> {

    public static final BinlogDataFactory INSTANCE = new BinlogDataFactory();

    @Override public BinlogData newInstance() {
        return new BinlogData();
    }
}
