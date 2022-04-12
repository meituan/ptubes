package com.meituan.ptubes.reader.container.network;

import com.meituan.ptubes.common.exception.InitializationFailedException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.Container;
import com.meituan.ptubes.reader.container.common.config.netty.ServerConfig;
import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.network.handler.ChannelIdleTimeoutHandler;
import com.meituan.ptubes.reader.container.network.handler.MonitorRequestHandler;
import com.meituan.ptubes.reader.container.network.processor.monitor.MonitorProcessorChain;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import com.meituan.ptubes.reader.container.common.AbstractLifeCycle;

/**
 * Netty server, open read permissions to the outside world, handle some specified indicators
 */
public class MonitorServer extends AbstractLifeCycle {

	private static final Logger LOG = LoggerFactory.getLogger(MonitorServer.class);

	private ServerBootstrap serverBootstrap;
	private Channel serverChannel;
	private MonitorProcessorChain processorChain;
	private ChannelIdleTimeoutHandler channelIdleTimeoutHandler;
	private MonitorRequestHandler monitorRequestHandler;
	private final ServerConfig serverConfig;
	private final Container container;

	private EventLoopGroup bossGroup = new NioEventLoopGroup(1, new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r,"monitor-boss");
		}
	});
	private EventLoopGroup ioGroup = new NioEventLoopGroup(Math.max(2, Runtime.getRuntime().availableProcessors()), new ThreadFactory() {
		private final AtomicInteger IO_GROUP_ID_GENERATOR = new AtomicInteger(1);
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r,"monitor-io" + IO_GROUP_ID_GENERATOR.getAndIncrement());
		}
	});

	public MonitorServer(Container container, ServerConfig serverConfig) {
		this.container = container;
		this.serverConfig = serverConfig;
	}

	@Override
	public boolean start() {
		boolean res = super.start();
		if (res) {
			this.channelIdleTimeoutHandler = new ChannelIdleTimeoutHandler();
			this.processorChain = new MonitorProcessorChain(container.getCollectorManager(), container.getReaderTaskManager());
			this.monitorRequestHandler = new MonitorRequestHandler(this.processorChain);

			this.serverBootstrap = new ServerBootstrap();
			this.serverBootstrap.group(bossGroup, ioGroup)
				.channel(NioServerSocketChannel.class)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel socketChannel) throws Exception {
						ChannelPipeline pipeline = socketChannel.pipeline();

						pipeline.addFirst("idleStateHandler", new IdleStateHandler(60, 0, 0));
						pipeline.addAfter("idleStateHandler", "idleTimeoutHandler", channelIdleTimeoutHandler);
						pipeline.addLast("httpDecodeHandler", new HttpRequestDecoder()/*line:4096,header:8192,chunk:8192*/);
						pipeline.addLast("httpObjectAggregator", new HttpObjectAggregator(ContainerConstants.MAX_CONTENT_LENGTH));
						pipeline.addLast("httpCodecHandler", new HttpResponseEncoder());
						pipeline.addLast("httpCompressor", new HttpContentCompressor());
						pipeline.addLast("httpChunkHandler", new ChunkedWriteHandler()); // channel.write(ChunkedInput)
						pipeline.addLast("httpRequestHandler", monitorRequestHandler);
					}
				})
				.option(ChannelOption.SO_BACKLOG, ContainerConstants.SO_BACKLOG)
				.childOption(ChannelOption.SO_REUSEADDR, true)
				.childOption(ChannelOption.TCP_NODELAY, false)
				.childOption(ChannelOption.SO_KEEPALIVE, true)
				.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

			try {
				ChannelFuture future = this.serverBootstrap.bind(ContainerConstants.MONITOR_SERVER_PORT);
				future.sync();
				this.serverChannel = future.channel();
				LOG.info("Monitor server start up");
			} catch (InterruptedException ie) {
				LOG.error("An interruptedException was caught while initializing monitor server", ie);
				Thread.currentThread().interrupt();
				throw new InitializationFailedException(ie);
			} catch (Throwable te) {
				LOG.error("A throwable was caught while intializing monitor server", te);
				throw new InitializationFailedException(te);
			}
		}
		return res;
	}

	@Override
	public boolean stop() {
		boolean res = super.stop();

		if (res) {
			// How do channel and session ensure that the recovery is successful within a certain period of time?
			this.serverChannel.close()
				.awaitUninterruptibly();
			LOG.info("monitor server channel close!");

			this.bossGroup.shutdownGracefully();
			this.ioGroup.shutdownGracefully();
			LOG.info("monitor server shutdown!");
		}
		return res;
	}

}
