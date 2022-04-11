package com.meituan.ptubes.reader.storage.mem.buffer;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;


public class BinlogInfoFactory {
    public static BinlogInfo newDefaultBinlogInfo(SourceType sourceType) {
        if (SourceType.MySQL.equals(sourceType)) {
            return new MySQLBinlogInfo();
        } else {
            throw new UnsupportedOperationException("Unsupported sourceType: " + sourceType);
        }
    }

    public static BinlogInfo newBinlogInfoByStr(SourceType sourceType, String binlogInfoStr) {
        if (SourceType.MySQL.equals(sourceType)) {
            return new MySQLBinlogInfo(binlogInfoStr);
        } else {
            throw new UnsupportedOperationException("Unsupported sourceType: " + sourceType);
        }
    }

    public static BinlogInfo decode(SourceType sourceType, byte[] data) {
        if (SourceType.MySQL.equals(sourceType)) {
            BinlogInfo binlogInfo = new MySQLBinlogInfo();
            binlogInfo.decode(data);
            return binlogInfo;
        } else {
            throw new UnsupportedOperationException("Unsupported sourceType: " + sourceType);
        }
    }

    public static short getLength(SourceType sourceType) {
        if (SourceType.MySQL.equals(sourceType)) {
            return MySQLBinlogInfo.LENGTH;
        } else {
            throw new UnsupportedOperationException("Unsupported sourceType: " + sourceType);
        }
    }

    public static BinlogInfo genSearchBinlogInfo(SourceType sourceType, BinlogInfo originBinlogInfo) {
        if (originBinlogInfo instanceof MySQLBinlogInfo) {
            // no copy, return the same object
            return originBinlogInfo;
        } else {
            throw new UnsupportedOperationException("Unsupported sourceType: " + sourceType);
        }
    }
}
