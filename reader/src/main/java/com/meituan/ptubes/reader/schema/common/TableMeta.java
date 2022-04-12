package com.meituan.ptubes.reader.schema.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.meituan.ptubes.common.utils.StringUtil;
import com.meituan.ptubes.common.utils.TimeUtil;
import java.util.List;
import java.util.Objects;

public class TableMeta {

    private final String tag = "1.0";
    private String ctime;
    private String metaVersion;
    private String tableOwner;
    private String table;
    private List<FieldMeta> orderedFieldMetaList;
    private String pkFieldListStr;
    private String primaryKeyFieldListStr;
    @JsonIgnore
    private List<String> pkFieldNameList;
    @JsonIgnore
    private List<String> primaryKeyFieldNameList;

    public TableMeta() {

    }

    public TableMeta(
        String metaVersion,
        String tableOwner,
        String table,
        List<FieldMeta> orderedFieldMetaList,
        String pkFieldListStr,
        String primaryKeyFieldListStr
    ) {
        this.metaVersion = metaVersion;
        this.tableOwner = tableOwner;
        this.table = table;
        this.orderedFieldMetaList = orderedFieldMetaList;
        this.pkFieldListStr = pkFieldListStr;
        this.primaryKeyFieldListStr = primaryKeyFieldListStr;
        this.ctime = TimeUtil.timeStr(System.currentTimeMillis());
    }

    public String getTag() {
        return tag;
    }

    public String getCtime() {
        return ctime;
    }

    public String getMetaVersion() {
        return metaVersion;
    }

    public String getTableOwner() {
        return tableOwner;
    }

    public String getTable() {
        return table;
    }

    public List<FieldMeta> getOrderedFieldMetaList() {
        return orderedFieldMetaList;
    }

    public String getPkFieldListStr() {
        return pkFieldListStr;
    }

    public String getPrimaryKeyFieldListStr() {
        return primaryKeyFieldListStr;
    }

    @JsonIgnore
    public List<String> getPkFieldNameList() {
        if (Objects.isNull(pkFieldNameList)) {
            synchronized (this) {
                if (Objects.isNull(pkFieldNameList)) {
                    pkFieldNameList = StringUtil.stringListBreak(pkFieldListStr, ",");
                }
            }
        }
        return pkFieldNameList;
    }

    @JsonIgnore
    public List<String> getPrimaryKeyFieldNameList() {
        if (Objects.isNull(primaryKeyFieldNameList)) {
            synchronized (this) {
                if (Objects.isNull(primaryKeyFieldNameList)) {
                    primaryKeyFieldNameList = StringUtil.stringListBreak(primaryKeyFieldListStr, ",");
                }
            }
        }
        return primaryKeyFieldNameList;
    }
}
