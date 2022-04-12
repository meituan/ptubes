package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.ChannelHandlerContext;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;

public class KeepAliveProcessor extends NonVerifiableProcessor<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(ServerInfoProcessor.class);
    protected static final String EXPECTED_PATH = "/monitor/alive";

    @Override
    public boolean isMatch(ClientRequest clientRequest) {
        return EXPECTED_PATH.equals(clientRequest.getPath());
    }

    @Override
    public Void decode(ChannelHandlerContext ctx, ClientRequest request) {
        return null;
    }

    @Override
    public boolean process0(ChannelHandlerContext ctx, Void request) {
        ctx.channel().writeAndFlush(NettyUtil.wrappedFullResponse("OK".getBytes()));
        return true;
    }

}
