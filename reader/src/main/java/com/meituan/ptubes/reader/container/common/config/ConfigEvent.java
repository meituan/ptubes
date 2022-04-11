package com.meituan.ptubes.reader.container.common.config;

public class ConfigEvent {

    private String key;
    private String oldValue;
    private String newValue;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getOldValue() {
        return oldValue;
    }

    public void setOldValue(String oldValue) {
        this.oldValue = oldValue;
    }

    public String getNewValue() {
        return newValue;
    }

    public void setNewValue(String newValue) {
        this.newValue = newValue;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(128);
        buf.append("ConfigEvent[key=").append(key).append(",oldValue=").append(oldValue)
            .append(",newValue=").append(newValue).append("]");
        return buf.toString();
    }
}
