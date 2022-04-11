package com.meituan.ptubes.reader.container.network.handler;

import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.group.ChannelGroup;
import java.net.InetSocketAddress;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.container.manager.SessionManager;

@ChannelHandler.Sharable
public class ConnectionHandler extends ChannelDuplexHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ConnectionHandler.class);

	private final ChannelGroup channelGroup;
	private final SessionManager sessionManager;

	public ConnectionHandler(ChannelGroup channelGroup, SessionManager sessionManager) {
		this.channelGroup = channelGroup;
		this.sessionManager = sessionManager;
	}

	// channelRegistered -> channelActive -> channelInactive -> channelUnregistered
	/**
	 * external close(taskQueue) ----------------------------------
	 * \
	 * eventLoop OP_READ(sub) --> fireChannelRead --> runTask(queue) --> close selector
	 * In this case, the sub request will be processed first, then closed, and executed serially, without parallel conflict
	 *
	 * external write --> close --------------------
	 * \
	 * eventLoop SELECT_KEY --> fireEvent --> runTask --> write(need flush) --> close The data may be discarded before it is sent to the socket.
	 * Added flush to ensure data output, see AbstractChannel#close for details
	 */

	/**
	 *
	 * @param ctx
	 * @throws Exception
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		channelGroup.add(ctx.channel());
		ctx.fireChannelActive();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		/**
		 * some test cases:
		 * 1. request handle error -> session stop -> channel close -> trig channel inactive -> session deregister(skip), all run in EventLoop
		 * 2. client unsub -> session deregister -> session stop -> channel close -> trig channel inactive -> session deregister(skip), all run in EventLoop
		 * 2. client disconnect -> trig channel inactive -> session deregister -> session close, all run in EventLoop
		 * 3. container close(shutdownHook) -> sessionmanager stop -> session deregister -> session stop(shutdownHook) -> channel close(eventLoop) -> trig channel inactive -> session stop(skip), run in different thread
		 */
		Channel channel = ctx.channel();
		String clientId = NettyUtil.getChannelAttribute(channel, ContainerConstants.ATTR_CLIENTID);
		if (null == clientId) {
			if (LOG.isDebugEnabled()) {
				InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
				String ip = socketAddress.getAddress().getHostAddress();
				LOG.warn("channel {}, ip {} has no clientId, close normally", channel.id().asShortText(), ip);
			}
		} else {
			LOG.info("start to deregister session for clientId {}", clientId);
			sessionManager.deregisterAsync(clientId);
		}
		channelGroup.remove(ctx.channel());
		ctx.fireChannelInactive();
	}

	// The difference between Disconnect and close is the underlying implementation
    
    // disconnect -> channelInactive -> channelUnregistered(The cancellation of selectionKey)
    // Close: Can allow multiple close, promise results are consistent, close process can not forget the channel write data
    // close -> channelInactive -> channelUnregistered(The cancellation of selectionKey)
	@Override
	public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise)
			throws Exception {
		// channel id look like a45e60fffed777e1-0000d54f-00000001-f645323bcfa2ff65-5997109b(for long) or a45e60fffed777e1(for short), global unique
        Channel channel = ctx.channel();
        if (LOG.isDebugEnabled()) {
            LOG.debug("client {} disconnect from server", channel.id().asShortText());
        } else {
            String clientId = NettyUtil.getChannelAttribute(channel, ContainerConstants.ATTR_CLIENTID);
            if (null != clientId) {
                LOG.info("client {} disconnect from server", channel.id().asShortText());
            }
        }
		ctx.disconnect(promise);
	}

	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        Channel channel = ctx.channel();
        if (LOG.isDebugEnabled()) {
            LOG.debug("server forwardly close client {}", channel.id().asShortText());
        } else {
            String clientId = NettyUtil.getChannelAttribute(channel, ContainerConstants.ATTR_CLIENTID);
            if (null != clientId) {
                LOG.info("server forwardly close client {}", channel.id().asShortText());
            }
        }
		ctx.close(promise);
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		InetSocketAddress socketAddress = (InetSocketAddress)ctx.channel().remoteAddress();
		String ip = socketAddress.getAddress().getHostAddress();
		LOG.debug("client {} ip={} connect!", ctx.channel().id().asShortText(), ip);
		ctx.fireChannelRegistered();
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		InetSocketAddress socketAddress = (InetSocketAddress)ctx.channel().remoteAddress();
		String ip = socketAddress.getAddress().getHostAddress();
		LOG.debug("client {} ip={} leave!", ctx.channel().id().asShortText(), ip);
		ctx.fireChannelUnregistered();
	}

}
