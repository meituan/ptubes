package com.meituan.ptubes.sdk.netty;

import com.meituan.ptubes.sdk.model.DataRequest;
import com.meituan.ptubes.sdk.model.ServerInfo;
import com.meituan.ptubes.sdk.config.ReaderConnectionConfig;
import com.meituan.ptubes.sdk.consumer.FetchThreadState;

public interface RdsCdcReaderConnection extends RdsCdcServerConnection {

    ServerInfo connect();

    void subscribe(ReaderConnectionConfig readerConnectionConfig);

    void fetchMessages(DataRequest dataRequest, FetchThreadState fetchThreadState);
}
