package com.meituan.ptubes.reader.container.network.handler;

import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;

/**
 * Timeout log management
 */
@ChannelHandler.Sharable
public class ChannelIdleTimeoutHandler extends ChannelDuplexHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ChannelIdleTimeoutHandler.class);

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleState e = ((IdleStateEvent) evt).state();
			if (e == IdleState.ALL_IDLE) {
				// it is better to print ctx.channel().remoteAddress()?
				LOG.info("channel {} rw idle timeout",
						 (String)NettyUtil.getChannelAttribute(ctx.channel(), ContainerConstants.ATTR_CLIENTID));
				ctx.close();
			}
		} else {
			LOG.info("channel {} trig event {}", ctx.channel().attr(AttributeKey.valueOf(ContainerConstants.ATTR_CLIENTID)),
					evt.getClass().getName());
		}
		super.userEventTriggered(ctx, evt);
	}

}
