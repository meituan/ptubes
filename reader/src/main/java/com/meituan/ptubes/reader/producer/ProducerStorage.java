package com.meituan.ptubes.reader.producer;

import com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import org.apache.commons.lang3.tuple.Pair;

public interface ProducerStorage {

    void switchStorageMode(StorageConstant.StorageMode storageMode) throws InterruptedException;

    Pair<BinlogInfo, BinlogInfo> getBinlogInfoRange();

    StorageConstant.StorageRangeCheckResult isInStorageRange(BinlogInfo binlogInfo);

    ProducerConfig getProducerConfig();

    StorageConfig getStorageConfig();

    void pushForwardCheckpoint(BinlogInfo binlogInfo);
}
