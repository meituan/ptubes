package com.meituan.ptubes.sdk.consumer;

import com.meituan.ptubes.sdk.model.ReaderServerInfo;
import com.meituan.ptubes.sdk.model.ServerInfo;
import java.util.List;
import com.meituan.ptubes.sdk.config.ReaderConnectionConfig;

public interface IReaderRebalancer {

    ServerInfo chooseCandidateServer(
        List<ReaderServerInfo> sgServices,
        ServerInfo connectReader,
        ReaderConnectionConfig readerConnectionConfig
    );

}
