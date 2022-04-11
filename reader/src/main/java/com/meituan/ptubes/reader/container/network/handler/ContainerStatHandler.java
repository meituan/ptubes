package com.meituan.ptubes.reader.container.network.handler;

import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.monitor.collector.ClientSessionStatMetricsCollector;
import com.meituan.ptubes.reader.monitor.collector.ContainerStatMetricsCollector;

@ChannelHandler.Sharable
public class ContainerStatHandler extends ChannelDuplexHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ContainerStatHandler.class);

	private final ContainerStatMetricsCollector containerStatMetricsCollector;

	public ContainerStatHandler(ContainerStatMetricsCollector collector) {
		this.containerStatMetricsCollector = collector;
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleState e = ((IdleStateEvent) evt).state();
			if (e == IdleState.READER_IDLE) {
				containerStatMetricsCollector.incrIdleTimeoutConnectionNum();
			}
		} else {
			
		}
		super.userEventTriggered(ctx, evt);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		int size = 0;
		Channel channel = ctx.channel();
		if (msg instanceof ByteBuf) {
			size = ((ByteBuf) msg).readableBytes();
		} else if (msg instanceof ByteBufHolder) {
			size = ((ByteBufHolder) msg).content().readableBytes();
		}
		containerStatMetricsCollector.accumulateInboundTraffic(size);
		ClientSessionStatMetricsCollector clientSessionCollector = NettyUtil.getChannelAttribute(channel, ContainerConstants.ATTR_CLIENTSESSION_COLLECTOR);
		if (clientSessionCollector != null) {
			clientSessionCollector.accumulateInboundTraffic(size);
		}
		ctx.fireChannelRead(msg);
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		int size = 0;
		Channel channel = ctx.channel();
		if (msg instanceof ByteBuf) {
			size = ((ByteBuf) msg).readableBytes();
		} else if (msg instanceof ByteBufHolder) {
			size = ((ByteBufHolder) msg).content().readableBytes();
		}
		containerStatMetricsCollector.accumulateOutboundTraffic(size);
		ClientSessionStatMetricsCollector clientSessionCollector = NettyUtil.getChannelAttribute(channel, ContainerConstants.ATTR_CLIENTSESSION_COLLECTOR);
		if (clientSessionCollector != null) {
			clientSessionCollector.accumulateOutboundTraffic(size);
		}
		ctx.write(msg, promise);
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		containerStatMetricsCollector.incrActiveConnectionNum();
		ctx.fireChannelRegistered();
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		containerStatMetricsCollector.decrActiveConnectionNum();
		ctx.fireChannelUnregistered();
	}

}
