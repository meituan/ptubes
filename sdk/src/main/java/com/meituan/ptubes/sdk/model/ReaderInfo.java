package com.meituan.ptubes.sdk.model;

import java.util.List;

public class ReaderInfo {
    public ReaderInfo() {
    }

    public ReaderInfo(List<ReaderServerInfo> readers) {
        this.readers = readers;
    }

    List<ReaderServerInfo> readers;

    public List<ReaderServerInfo> getReaders() {
        return readers;
    }

    public void setReaders(List<ReaderServerInfo> readers) {
        this.readers = readers;
    }
}
