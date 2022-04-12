package com.meituan.ptubes.reader.container.network.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

@ChannelHandler.Sharable
public class ExceptionHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(ExceptionHandler.class);

	// The exception behavior has been handled in the processor, this handler can be removed, or receive unknown exceptions
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		LOG.error("exception caught", cause);
		ctx.fireExceptionCaught(cause);
	}

}
