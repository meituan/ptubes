package com.meituan.ptubes.sdk.reader;

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
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public class ReaderServer {

    private static final Logger LOG = LoggerFactory.getLogger(ReaderServer.class);

    private ServerBootstrap serverBootstrap;
    private Channel serverChannel;
    private ChannelGroup channelGroup;

    private EventLoopGroup bossGroup = new NioEventLoopGroup(
        1,
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(
                    r,
                    "server-boss"
                );
            }
        }
    );
    private EventLoopGroup ioGroup = new NioEventLoopGroup(
        Math.max(
            2,
            Runtime.getRuntime()
                .availableProcessors() * 2 + 1
        ),
        new ThreadFactory() {
            private final AtomicInteger IO_GROUP_ID_GENERATOR = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(
                    r,
                    "server-io" + IO_GROUP_ID_GENERATOR.getAndIncrement()
                );
            }
        }
    );

    public void start() {
        this.channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        this.serverBootstrap = new ServerBootstrap();
        this.serverBootstrap.group(
            bossGroup,
            ioGroup
        )
            .channel(NioServerSocketChannel.class)
            .childHandler(new HttpServerPipelineInitializer())
            .option(
                ChannelOption.SO_BACKLOG,
                ContainerConstants.SO_BACKLOG
            )
            .childOption(
                ChannelOption.SO_REUSEADDR,
                ContainerConstants.SO_REUSEADDR
            )
            .childOption(
                ChannelOption.TCP_NODELAY,
                ContainerConstants.TCP_NODELAY
            )
            .childOption(
                ChannelOption.SO_KEEPALIVE,
                ContainerConstants.SO_KEEPALIVE
            )
            .childOption(
                ChannelOption.ALLOCATOR,
                PooledByteBufAllocator.DEFAULT
            );
        try {
            ChannelFuture future = this.serverBootstrap.bind(ContainerConstants.DATA_SERVER_PORT);
            LOG.info(
                "Server has been bound port={}",
                ContainerConstants.DATA_SERVER_PORT
            );
            future.sync();
            this.serverChannel = future.channel();
        } catch (InterruptedException ie) {
            LOG.error(
                "An interruptedException was caught while initializing server",
                ie
            );
            Thread.currentThread()
                .interrupt();
            throw new RuntimeException(ie);
        } catch (Throwable te) {
            LOG.error(
                "A throwable was caught while intializing server",
                te
            );
            throw new RuntimeException(te);
        }
    }

    public void stop() {
        this.serverChannel.close()
            .awaitUninterruptibly();
        LOG.info("server channel close!");

        int channelNum = channelGroup.size();
        this.channelGroup.close()
            .awaitUninterruptibly();
        LOG.info(
            "{} children channels are all closed!",
            channelNum
        );

        this.bossGroup.shutdownGracefully();
        this.ioGroup.shutdownGracefully();
        LOG.info("server shutdown!");
    }

}
