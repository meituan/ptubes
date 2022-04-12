package com.meituan.ptubes.reader.container.common.constants;

import javax.annotation.Nullable;

public enum SourceType {

    MySQL((byte)1);

    private byte sourceTypeCode;
    SourceType(byte sourceTypeCode) {
        this.sourceTypeCode = sourceTypeCode;
    }

    public byte getSourceTypeCode() {
        return sourceTypeCode;
    }

    @Nullable
    public static SourceType valueOf(byte sourceTypeCode) {
        for (SourceType sourceType : SourceType.values()) {
            if (sourceType.getSourceTypeCode() == sourceTypeCode) {
                return sourceType;
            }
        }
        return null;
    }
}
