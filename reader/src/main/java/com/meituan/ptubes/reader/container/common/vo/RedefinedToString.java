package com.meituan.ptubes.reader.container.common.vo;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class RedefinedToString {
    @Override
    public String toString() {
        return new ReflectionToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE) {
            @Override
            public ToStringBuilder append(String fieldName, Object obj) {
                if (obj != null) {
                    return super.append(fieldName, obj);
                }
                return this;
            }
        }.toString();
    }
}
