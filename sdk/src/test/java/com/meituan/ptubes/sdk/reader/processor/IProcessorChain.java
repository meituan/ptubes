package com.meituan.ptubes.sdk.reader.processor;

import io.netty.channel.ChannelHandlerContext;
import com.meituan.ptubes.sdk.reader.bean.ClientRequest;

public interface IProcessorChain {

    void process(
        ChannelHandlerContext ctx,
        ClientRequest clientRequest
    );

}
