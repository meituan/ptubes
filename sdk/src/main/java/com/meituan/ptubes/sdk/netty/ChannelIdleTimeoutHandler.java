package com.meituan.ptubes.sdk.netty;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

@ChannelHandler.Sharable
public class ChannelIdleTimeoutHandler extends ChannelDuplexHandler {

    private final Logger log;

    public ChannelIdleTimeoutHandler(String taskName) {
        this. log = LoggerFactory.getLoggerByTask(ChannelIdleTimeoutHandler.class, taskName);
    }

    @Override
    public void userEventTriggered(
        ChannelHandlerContext ctx,
        Object evt
    ) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleState e = IdleStateEvent.class.cast(evt)
                .state();
            if (IdleState.ALL_IDLE.equals(e)) {
                log.info(ctx.name() + " channel rw idle timeout, closing channel.");
                ctx.close();
            }
        }
        super.userEventTriggered(
            ctx,
            evt
        );
    }

}
