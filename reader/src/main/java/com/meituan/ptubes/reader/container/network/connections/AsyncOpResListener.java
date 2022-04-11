package com.meituan.ptubes.reader.container.network.connections;

public interface AsyncOpResListener {

    void operationComplete(boolean isSuccess);

}
