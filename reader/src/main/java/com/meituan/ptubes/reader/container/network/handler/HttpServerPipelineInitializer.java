package com.meituan.ptubes.reader.container.network.handler;

import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import com.meituan.ptubes.reader.container.network.ReaderServer;

public class HttpServerPipelineInitializer extends ChannelInitializer<SocketChannel> {

	private final ReaderServer parent;

	public HttpServerPipelineInitializer(ReaderServer server) {
		this.parent = server;
	}

	@Override
	protected void initChannel(SocketChannel socketChannel) throws Exception {
		ChannelPipeline pipeline = socketChannel.pipeline();

		pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, ContainerConstants.CHANNEL_IDLE_TIMEOUT));
		pipeline.addAfter("idleStateHandler", "idleTimeoutHandler", parent.getChannelIdleTimeoutHandler());
		pipeline.addLast("containerStatHandler", parent.getContainerStatHandler());
		pipeline.addLast("connectionHandler", parent.getConnectionHandler());
		pipeline.addLast("httpDecodeHandler", new HttpRequestDecoder(parent.getServerConfig().getMaxInitialLineLength(),
				parent.getServerConfig().getMaxHeaderSize(), parent.getServerConfig().getMaxChunkSize()));
//		pipeline.addLast("httpDecompressor", new HttpContentDecompressor()); // just work for response
		pipeline.addLast("httpObjectAggregator", new HttpObjectAggregator(parent.getServerConfig().getMaxContentLength()));
		pipeline.addLast("httpCodecHandler", new HttpResponseEncoder());
		pipeline.addLast("httpCompressor", new HttpContentCompressor());
//		pipeline.addLast("httpChunkHandler", new ChunkedWriteHandler()); // use chunked response if neccessary
		pipeline.addLast("httpRequestHandler", parent.getHttpRequestHandler());
		pipeline.addLast("exceptionHandler", parent.getExceptionHandler());
	}

}
