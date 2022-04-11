package com.meituan.ptubes.reader.container.common.vo;

public interface MaxBinlogInfo {

    /**
     * Need to override the toString method for disk storage
     * @return
     */
    @Override String toString();

    String SEPERATOR = "\n";
}
