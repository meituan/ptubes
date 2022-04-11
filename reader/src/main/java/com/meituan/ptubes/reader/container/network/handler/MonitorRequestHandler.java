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
public class MonitorRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorRequestHandler.class);

    private final IProcessorChain processorChain;

    public MonitorRequestHandler(IProcessorChain processorChain) {
        this.processorChain = processorChain;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        HttpMethod method = msg.method();

        QueryStringDecoder uriDecoder = new QueryStringDecoder(msg.uri());
        String path = uriDecoder.path();
        Map<String, List<String>> pathVariables = uriDecoder.parameters();
        HttpHeaders headers = msg.headers();
        ByteBuf body = HttpMethod.POST.equals(method) ? msg.content() : null;
        ClientRequest clientRequest = new ClientRequest(method, path, pathVariables, headers, body);
        processorChain.process(ctx, clientRequest);
    }

}
