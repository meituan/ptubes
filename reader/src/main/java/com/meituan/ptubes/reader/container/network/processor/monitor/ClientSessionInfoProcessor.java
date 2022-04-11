package com.meituan.ptubes.reader.container.network.processor.monitor;

import com.meituan.ptubes.reader.container.network.processor.NonVerifiableProcessor;
import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.monitor.collector.AggregatedClientSessionStatMetricsCollector;

public class ClientSessionInfoProcessor extends NonVerifiableProcessor<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(BasicInfoProcessor.class);

    private static final String EXPECTED_PATH = "/v1/clientSessionInfo";

    private final AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector;
    private final ExecutorService executors;

    public ClientSessionInfoProcessor(AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector) {
        this.aggregatedClientSessionStatMetricsCollector = aggregatedClientSessionStatMetricsCollector;
        this.executors = null;
    }

    public ClientSessionInfoProcessor(
        AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector,
        ExecutorService executors
    ) {
        this.aggregatedClientSessionStatMetricsCollector = aggregatedClientSessionStatMetricsCollector;
        this.executors = executors;
    }

    @Override
    public boolean isMatch(ClientRequest clientRequest) {
        return EXPECTED_PATH.equals(clientRequest.getPath());
    }

    @Override
    public Void decode(ChannelHandlerContext ctx, ClientRequest request) {
        return null;
    }

    @Override
    public boolean process0(ChannelHandlerContext ctx, Void request) {
        Channel channel = ctx.channel();

        DefaultFullHttpResponse response;
        try {
            response = NettyUtil.wrappedFullResponse(JSONUtil.toJsonString(
                aggregatedClientSessionStatMetricsCollector.toClientSessionInfos(),
                false
            ).getBytes());
        } catch (IOException ioe) {
            LOG.warn("transfer basic info to json string error", ioe);
            response = NettyUtil.wrappedFullResponse("{}".getBytes());
        }

        ctx.writeAndFlush(response);
        return true;
    }

}
