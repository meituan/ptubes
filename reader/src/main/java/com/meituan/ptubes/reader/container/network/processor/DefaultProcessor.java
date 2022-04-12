package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;

public class DefaultProcessor extends NonVerifiableProcessor<Void> {

    @Override
    public boolean isMatch(ClientRequest clientRequest) {
        // default skipping
        return true;
    }
    /**
     * No parsing required
     *
     * @param ctx
     * @param request
     * @return
     */
    @Override protected Void decode(ChannelHandlerContext ctx, ClientRequest request) {
        return null;
    }

    @Override protected boolean process0(ChannelHandlerContext ctx, Void request) {
        ctx.channel().writeAndFlush(NettyUtil.wrappedFullResponse("Bye".getBytes())).addListener(ChannelFutureListener.CLOSE);
        return false;
    }
}
