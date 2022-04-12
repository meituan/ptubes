package com.meituan.ptubes.reader.schema.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.meituan.ptubes.reader.schema.util.SchemaPrimitiveTypes;
import com.meituan.ptubes.reader.container.common.vo.PtubesFieldType;

public class FieldMeta {

    private int index;
    private String name;
    private String dbType;
    private String defaultValue;
    @JsonProperty("unsigned")
    private boolean isUnsigned;
    @JsonProperty("nullable")
    private boolean isNullable;
    @JsonProperty("primaryKey")
    private boolean isPrimaryKey;
    @JsonProperty("partitionKey")
    private boolean isPartitionKey;
    private int numbericPrecision;
    private int numbericScale;
    private int datetimePrecision;
    private SchemaPrimitiveTypes primitiveType;

    public FieldMeta() {

    }

    public FieldMeta(
        int index,
        String name,
        String dbType,
        SchemaPrimitiveTypes primitiveType,
        String defaultValue,
        boolean isUnsigned,
        boolean isNullable,
        boolean isPrimaryKey,
        boolean isPartitionKey,
        int numbericPrecision,
        int numbericScale,
        int datetimePrecision
    ) {
        this.index = index;
        this.name = name;
        this.dbType = dbType;
        this.primitiveType = primitiveType;
        this.defaultValue = defaultValue;
        this.isUnsigned = isUnsigned;
        this.isNullable = isNullable;
        this.isPrimaryKey = isPrimaryKey;
        this.isPartitionKey = isPartitionKey;
        this.numbericPrecision = numbericPrecision;
        this.numbericScale = numbericScale;
        this.datetimePrecision = datetimePrecision;
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }

    public String getDbType() {
        return dbType;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isUnsigned() {
        return isUnsigned;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public boolean isPartitionKey() {
        return isPartitionKey;
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

    public SchemaPrimitiveTypes getPrimitiveType() {
        return primitiveType;
    }

    @JsonIgnore
    public PtubesFieldType getPtubesFieldType() {
        return getPrimitiveType().getPtubesFieldType();
    }
}
