package com.meituan.ptubes.reader.container.network.processor.monitor;

import com.meituan.ptubes.reader.container.network.processor.NonVerifiableProcessor;
import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import java.util.concurrent.ExecutorService;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.monitor.collector.AggregatedReadTaskStatMetricsCollector;


public class ReaderTaskInfoProcessor extends NonVerifiableProcessor<Void> {
	private static final Logger LOG = LoggerFactory.getLogger(ReaderTaskInfoProcessor.class);

	private static final String EXPECTED_PATH = "/v1/readerTaskInfo";

	private final AggregatedReadTaskStatMetricsCollector aggregatedReadTaskStatMetricsCollector;
	private final ExecutorService executors;

	public ReaderTaskInfoProcessor(AggregatedReadTaskStatMetricsCollector aggregatedReadTaskStatMetricsCollector) {
		this.aggregatedReadTaskStatMetricsCollector = aggregatedReadTaskStatMetricsCollector;
		this.executors = null;
	}

	public ReaderTaskInfoProcessor(AggregatedReadTaskStatMetricsCollector aggregatedReadTaskStatMetricsCollector,
			ExecutorService executors) {
		this.aggregatedReadTaskStatMetricsCollector = aggregatedReadTaskStatMetricsCollector;
		this.executors = executors;
	}

	@Override
	public boolean isMatch(ClientRequest clientRequest) {
		return EXPECTED_PATH.equals(clientRequest.getPath());
	}

	@Override
	protected Void decode(ChannelHandlerContext ctx, ClientRequest request) {
		return null;
	}

	@Override
	protected boolean process0(ChannelHandlerContext ctx, Void request) {
		DefaultFullHttpResponse response;
		try {
			response = NettyUtil.wrappedFullResponse(JSONUtil.toJsonString(
					aggregatedReadTaskStatMetricsCollector.toReaderTaskInfos(),
					false
			).getBytes());
		} catch (Throwable te) {
			LOG.warn("transfer reader task info to json string error", te);
			response = NettyUtil.wrappedFullResponse("{}".getBytes());
		}

		ctx.writeAndFlush(response);
		return true;
	}
}
