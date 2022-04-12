package com.meituan.ptubes.reader.container.common.vo;

import com.google.protobuf.GeneratedMessageV3;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import java.util.Map;

public interface BinlogInfo {

    int getLength();
    SourceType getSourceType();

    byte[] encode();
    /** concrete type concrete implementation **/
    void decode(byte[] data);
    boolean isGreaterThan(BinlogInfo other, StorageConstant.IndexPolicy indexPolicy);
    boolean isEqualTo(BinlogInfo other, StorageConstant.IndexPolicy indexPolicy);
    boolean isGreaterEqualThan(BinlogInfo other, StorageConstant.IndexPolicy indexPolicy);
    boolean isValid();
    BinlogInfoComparison compare(BinlogInfo other, StorageConstant.IndexPolicy indexPolicy);

    /** transformer **/
    Map<String, Object> toMap();
    GeneratedMessageV3 toCheckpoint();

    long getTs();

    long getIncrementalLabel();
}
