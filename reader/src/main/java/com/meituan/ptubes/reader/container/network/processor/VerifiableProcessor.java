package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import com.meituan.ptubes.reader.container.network.request.Token;
import io.netty.channel.ChannelHandlerContext;

public abstract class VerifiableProcessor<T extends Token> implements IProcessor {

	@Override
	public boolean isMatch(ClientRequest clientRequest) {
		// default skipping
		return false;
	}

	@Override
	public boolean process(ChannelHandlerContext ctx, ClientRequest request) {
		T req = decode(ctx, request);
		if (verify(req)) {
			return process0(ctx, req);
		} else {
			// return client verify exception
			return false;
		}
	}

	protected boolean verify(Token token) {
		
		return true;
	}

	protected abstract T decode(ChannelHandlerContext ctx, ClientRequest request);
	protected abstract boolean process0(ChannelHandlerContext ctx, T request);

}
