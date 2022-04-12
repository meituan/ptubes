package com.meituan.ptubes.reader.container.network;

import com.meituan.ptubes.common.exception.InitializationFailedException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.Container;
import com.meituan.ptubes.reader.container.common.config.netty.ServerConfig;
import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.network.handler.ChannelIdleTimeoutHandler;
import com.meituan.ptubes.reader.container.network.handler.ConnectionHandler;
import com.meituan.ptubes.reader.container.network.handler.ContainerStatHandler;
import com.meituan.ptubes.reader.container.network.handler.ExceptionHandler;
import com.meituan.ptubes.reader.container.network.handler.HttpRequestHandler;
import com.meituan.ptubes.reader.container.network.handler.HttpServerPipelineInitializer;
import com.meituan.ptubes.reader.container.network.processor.ProcessorChain;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import com.meituan.ptubes.reader.container.common.AbstractLifeCycle;

/**
 * Netty Server, returns the server-side running metrics
 */
public class ReaderServer extends AbstractLifeCycle {

	private static final Logger LOG = LoggerFactory.getLogger(ReaderServer.class);

	private final Container container;
	private final ServerConfig serverConfig; // read static/dynamic config once by container
	private ServerBootstrap serverBootstrap;
	private Channel serverChannel;
	// Map<WriteTaskName, ChannelGroup>
	private ChannelGroup channelGroup;
	private ProcessorChain processorChain;
	// sharable handler
	private ChannelIdleTimeoutHandler channelIdleTimeoutHandler;
	private ConnectionHandler connectionHandler;
	private ContainerStatHandler containerStatHandler;
	private ExceptionHandler exceptionHandler;
	private HttpRequestHandler httpRequestHandler;

	public ServerConfig getServerConfig() {
		return serverConfig;
	}

	public ProcessorChain getProcessorChain() {
		return processorChain;
	}

	public ChannelIdleTimeoutHandler getChannelIdleTimeoutHandler() {
		return channelIdleTimeoutHandler;
	}

	public ConnectionHandler getConnectionHandler() {
		return connectionHandler;
	}

	public ContainerStatHandler getContainerStatHandler() {
		return containerStatHandler;
	}

	public ExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	public HttpRequestHandler getHttpRequestHandler() {
		return httpRequestHandler;
	}

	public ReaderServer(Container container, ServerConfig config) {
		this.container = container;
		this.serverConfig = config;
	}

	private EventLoopGroup bossGroup = new NioEventLoopGroup(1, new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r,"server-boss");
		}
	});
	private EventLoopGroup ioGroup = new NioEventLoopGroup(Math.max(2, Runtime.getRuntime().availableProcessors() * 2 + 1), new ThreadFactory() {
		private final AtomicInteger IO_GROUP_ID_GENERATOR = new AtomicInteger(1);
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r,"server-io" + IO_GROUP_ID_GENERATOR.getAndIncrement());
		}
	});

	@Override
	public boolean start() {
		boolean res = super.start();

		if (res) {
			this.channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
			this.channelIdleTimeoutHandler = new ChannelIdleTimeoutHandler();
			this.connectionHandler = new ConnectionHandler(this.channelGroup, container.getSessionManager());
			this.containerStatHandler = new ContainerStatHandler(container.getCollectorManager().getContainerStatMetricsCollector());
			this.exceptionHandler = new ExceptionHandler();
			this.processorChain = new ProcessorChain(container.getSessionManager(), container.getReaderTaskManager(), container.getCollectorManager());
			this.httpRequestHandler = new HttpRequestHandler(this.processorChain);

			this.serverBootstrap = new ServerBootstrap();
			this.serverBootstrap.group(bossGroup, ioGroup)
				.channel(NioServerSocketChannel.class)
				.childHandler(new HttpServerPipelineInitializer(this))
				.option(ChannelOption.SO_BACKLOG, ContainerConstants.SO_BACKLOG)
				.childOption(ChannelOption.SO_REUSEADDR, ContainerConstants.SO_REUSEADDR)
				.childOption(ChannelOption.TCP_NODELAY, ContainerConstants.TCP_NODELAY)
				.childOption(ChannelOption.SO_KEEPALIVE, ContainerConstants.SO_KEEPALIVE)
				.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
			try {
				ChannelFuture future = this.serverBootstrap.bind(serverConfig.getDataPort());
				LOG.info("Server has been bound port={}", serverConfig.getDataPort());
				// wait for setup
				future.sync();
				this.serverChannel = future.channel();
			} catch (InterruptedException ie) {
				LOG.error("An interruptedException was caught while initializing server", ie);
				Thread.currentThread().interrupt();
				throw new InitializationFailedException(ie);
			} catch (Throwable te) {
				LOG.error("A throwable was caught while intializing server", te);
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
			this.serverChannel.close().awaitUninterruptibly();
			LOG.info("server channel close!");

			// recycle session in addListener or channelInactive event
			int channelNum = channelGroup.size();
			this.channelGroup.close().awaitUninterruptibly();
			LOG.info("{} children channels are all closed!", channelNum);

			this.bossGroup.shutdownGracefully();
			this.ioGroup.shutdownGracefully();
			LOG.info("server shutdown!");
		}
		return res;
	}

}
