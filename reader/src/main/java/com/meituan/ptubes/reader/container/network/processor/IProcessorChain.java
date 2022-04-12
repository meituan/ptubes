package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.ChannelHandlerContext;

public interface IProcessorChain {

    void process(ChannelHandlerContext ctx, ClientRequest clientRequest);

}
