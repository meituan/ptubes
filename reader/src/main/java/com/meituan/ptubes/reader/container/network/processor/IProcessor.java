package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.ChannelHandlerContext;

public interface IProcessor {

	boolean isMatch(ClientRequest clientRequest);

	boolean process(ChannelHandlerContext ctx, ClientRequest request);

}
