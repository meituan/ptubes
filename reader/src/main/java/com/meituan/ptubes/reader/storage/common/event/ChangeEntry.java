package com.meituan.ptubes.reader.storage.common.event;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import javax.annotation.Nonnull;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MaxBinlogInfo;

public interface ChangeEntry {

    byte getVersion();

    SourceType getSourceType();

    BinlogInfo getBinlogInfo();

    MaxBinlogInfo getMaxBinlogInfo();

    Object genEventKey() throws PtubesException;

    
    String getTableName();
    EventType getEventType();
    long getFromServerIp();
    long getTimestamp();

    @Nonnull
    byte[] getSerializedExtraFlags();

    @Nonnull
    byte[] getSerializedRecord();

    @Nonnull
    byte[] getSerializedChangeEntry();

    long getCommitTs();

    long getReceiveNanos();

    /**
     * schema id It is only compatible with the previous format, and it is unlikely to be used again. After all, it is devolved to business application analysis, and the coupling of data and protocol is inconvenient.
     * @return
     */
    @Deprecated
    default int getSchemaId() {
        return -1;
    }

    boolean canAppend(BinlogInfo lastWrittenBinlogInfo, StorageConstant.IndexPolicy indexPolicy);

}
