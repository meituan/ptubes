package com.meituan.ptubes.reader.schema.common;

public class ColumnMetaData {

    private final int pos;
    private final String name;
    private final String defValue;
    private final boolean isNullable;
    private final String dataType;
    private final int numbericPrecision;
    private final int numbericScale;
    private final int datetimePrecision;
    private final String characterSetName;
    private final boolean isUnsigned;

    public ColumnMetaData(int pos, String name, String defValue, boolean isNullable, String dataType, int numbericPrecision,
        int numbericScale, int datetimePrecision, String characterSetName, boolean isUnsigned) {
        this.pos = pos;
        this.name = name;
        this.defValue = defValue;
        this.isNullable = isNullable;
        this.dataType = dataType;
        this.numbericPrecision = numbericPrecision;
        this.numbericScale = numbericScale;
        this.datetimePrecision = datetimePrecision;
        this.characterSetName = characterSetName;
        this.isUnsigned = isUnsigned;
    }

    public int getPos() {
        return pos;
    }

    public String getName() {
        return name;
    }

    public String getDefValue() {
        return defValue;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public String getDataType() {
        return dataType;
    }

    public int getNumbericPrecision() {
        return numbericPrecision;
    }

    public int getNumbericScale() {
        return numbericScale;
    }

    public int getDatetimePrecision() {
        return datetimePrecision;
    }

    public String getCharacterSetName() {
        return characterSetName;
    }

    public boolean isUnsigned() {
        return isUnsigned;
    }
}
