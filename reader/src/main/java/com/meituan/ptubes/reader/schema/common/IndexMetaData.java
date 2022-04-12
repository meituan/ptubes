package com.meituan.ptubes.reader.schema.common;

import java.util.Collection;
import java.util.List;

public class IndexMetaData {

    private final boolean isUnique;
    private final boolean isPrimary;
    private final String keyName;
    private final List<String> orderedCols;

    public IndexMetaData(boolean isUnique, boolean isPrimary, String keyName, List<String> orderedCols) {
        this.isUnique = isUnique;
        this.isPrimary = isPrimary;
        this.keyName = keyName;
        this.orderedCols = orderedCols;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public String getKeyName() {
        return keyName;
    }

    public List<String> getOrderedCols() {
        return orderedCols;
    }

    public void addIndexCols(Collection<String> indexCols) {
        orderedCols.addAll(indexCols);
    }
}
