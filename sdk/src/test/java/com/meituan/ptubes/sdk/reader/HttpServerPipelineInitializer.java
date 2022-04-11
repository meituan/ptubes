package com.meituan.ptubes.sdk.reader;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class HttpServerPipelineInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();

        pipeline.addLast(
            "httpDecodeHandler",
            new HttpRequestDecoder(
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE
            )
        );
        pipeline.addLast(
            "httpObjectAggregator",
            new HttpObjectAggregator(ContainerConstants.MAX_CONTENT_LENGTH)
        );
        pipeline.addLast(
            "httpCodecHandler",
            new HttpResponseEncoder()
        );
        pipeline.addLast(
            "httpCompressor",
            new HttpContentCompressor()
        );
        pipeline.addLast(
            "httpRequestHandler",
            new HttpRequestHandler()
        );
    }

}
