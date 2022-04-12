package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.ChannelHandlerContext;

public abstract class NonVerifiableProcessor<T> implements IProcessor {

	@Override
	public boolean isMatch(ClientRequest clientRequest) {
		// default skipping
		return false;
	}

	@Override
	public boolean process(ChannelHandlerContext ctx, ClientRequest request) {
		T req = decode(ctx, request);
		return process0(ctx, req);
	}

	protected abstract T decode(ChannelHandlerContext ctx, ClientRequest request);
	protected abstract boolean process0(ChannelHandlerContext ctx, T request);
}
