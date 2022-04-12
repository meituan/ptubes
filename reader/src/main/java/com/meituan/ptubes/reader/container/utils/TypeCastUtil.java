package com.meituan.ptubes.reader.container.utils;

import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.Gtid;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;

public class TypeCastUtil {

    public static BinlogInfo checkpointToBinlogInfo(BuffaloCheckpoint abstractCheckpoint) {
        if (abstractCheckpoint instanceof MysqlCheckpoint) {
            MysqlCheckpoint checkpoint = (MysqlCheckpoint) abstractCheckpoint;
            return new MySQLBinlogInfo((short)-1,
                checkpoint.getServerId(),
                checkpoint.getBinlogFile(),
                checkpoint.getBinlogOffset(),
                Gtid.getUuidByte(checkpoint.getUuid()),
                checkpoint.getTransactionId(),
                checkpoint.getEventIndex(),
                checkpoint.getTimestamp()
            );
        } else {
            throw new IllegalArgumentException("Unrecoverable checkpoint type: " + abstractCheckpoint.getClass().getName());
        }
    }

}
