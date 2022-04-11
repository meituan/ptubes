package com.meituan.ptubes.reader.container.network.handler;

import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.util.List;
import java.util.Map;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.network.processor.IProcessorChain;

@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

	private static final Logger LOG = LoggerFactory.getLogger(HttpRequestHandler.class);

	private final IProcessorChain processorChain;

	public HttpRequestHandler(IProcessorChain processorChain) {
		this.processorChain = processorChain;
	}

	@Override
	public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
		try {
			HttpMethod method = msg.method();

			QueryStringDecoder uriDecoder = new QueryStringDecoder(msg.uri());
			String path = uriDecoder.path();
			Map<String, List<String>> pathVariables = uriDecoder.parameters();
			HttpHeaders headers = msg.headers();
			// For now, only POST is supported
			ByteBuf body = HttpMethod.POST.equals(method) ? msg.content() : null;
			ClientRequest clientRequest = new ClientRequest(
				method,
				path,
				pathVariables,
				headers,
				body
			);
			processorChain.process(
				ctx,
				clientRequest
			);
		} catch (Exception e) {
			LOG.error("handle http request error", e);
		}
	}

}
