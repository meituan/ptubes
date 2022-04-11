package com.meituan.ptubes.reader.container.network.connections;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import io.netty.channel.Channel;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.manager.SessionManager;
import com.meituan.ptubes.reader.container.network.request.sub.SubRequest;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;

public class DefaultClientSessionFactory implements IClientSessionFactory {

    private static final DefaultClientSessionFactory INSTANCE = new DefaultClientSessionFactory();

    public static IClientSessionFactory getInstance(){
        return  INSTANCE;
    }

    @Override
    public AbstractClientSession produceSession(
        SessionManager sessionManager,
        String readerTaskName,
        SourceType sourceType,
        String clientId,
        Channel channel,
        SubRequest subRequest,
        StorageConfig storageConfig,
        ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector
    ) {
        return new Session(
            sessionManager,
            readerTaskName,
            sourceType,
            clientId,
            channel,
            subRequest,
            storageConfig,
            readerTaskStatMetricsCollector
        );
    }

}
