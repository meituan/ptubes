package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Optional;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.container.manager.ReaderTaskManager;
import com.meituan.ptubes.reader.container.manager.SessionManager;
import com.meituan.ptubes.reader.container.network.connections.AsyncOpResListener;
import com.meituan.ptubes.reader.container.network.encoder.EncoderType;
import com.meituan.ptubes.reader.container.network.request.sub.MySQLSourceSubRequest;
import com.meituan.ptubes.reader.container.network.request.sub.SubRequest;
import com.meituan.ptubes.reader.monitor.collector.ClientSessionStatMetricsCollector;
import com.meituan.ptubes.reader.producer.EventProducer;
import com.meituan.ptubes.sdk.model.ServiceGroupInfo;

public class SubscribtionProcessor extends VerifiableProcessor<SubRequest> {

	private static final Logger LOG = LoggerFactory.getLogger(SubscribtionProcessor.class);
	protected static final String EXPECTED_PATH = "/v1/subscribe";

	private final SessionManager sessionManager;
	private final ReaderTaskManager readerTaskManager;

	public SubscribtionProcessor(SessionManager sessionManager, ReaderTaskManager readerTaskManager) {
		this.sessionManager = sessionManager;
		this.readerTaskManager = readerTaskManager;
	}

	@Override
	public boolean isMatch(ClientRequest clientRequest) {
		return EXPECTED_PATH.equals(clientRequest.getPath());
	}

	@Override
	protected SubRequest decode(ChannelHandlerContext ctx, ClientRequest request) {
		String body = request.getBody().toString(Charset.forName("UTF-8"));

		LOG.info("channel {} sub req:{}", ctx.channel().id(), body);
		Optional<ServiceGroupInfo> serviceGroupInfo = JSONUtil.toSimpleColumnBean(body, "serviceGroupInfo", ServiceGroupInfo.class);
		if (serviceGroupInfo.isPresent() == false) {
			return null;
		}
		String readerTaskName = serviceGroupInfo.get().getServiceGroupName();
		EventProducer eventProducer = readerTaskManager.getEventProducer(readerTaskName);
		if (Objects.nonNull(eventProducer)) {
			SourceType sourceType = eventProducer.getSourceType();
			switch (sourceType) {
				case MySQL:
					Optional<MySQLSourceSubRequest> mySQLSourceSubRequest = JSONUtil.jsonToSimpleBean(body, MySQLSourceSubRequest.class);
					return mySQLSourceSubRequest.isPresent() ? mySQLSourceSubRequest.get() : null;
				default:
					return null;
			}
		} else {
			return null;
		}
	}

	@Override
	public boolean process0(ChannelHandlerContext ctx, SubRequest request) {
		long start = System.currentTimeMillis();
		final Channel channel = ctx.channel();
		try {
			if (request == null || EncoderType.PROTOCOL_BUFFER.equals(request.getCodec()) == false) {
				DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse("fail".getBytes());
				channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
				return false;
			}

			LOG.info("channel {} sub req:{}", channel.id(), request);
			String writeTaskName = request.getServiceGroupInfo().getTaskName();
			String clientId = writeTaskName + "_" + channel.id().asShortText();
			String readTaskName = request.getServiceGroupInfo()
				.getServiceGroupName();

			sessionManager.register(
                readerTaskManager,
				readTaskName,
				writeTaskName,
				clientId,
				channel,
				request,
				new AsyncOpResListener() {
					@Override
					public void operationComplete(boolean isSuccess) {
						long end = System.currentTimeMillis();
						ClientSessionStatMetricsCollector collector = NettyUtil.getChannelAttribute(
							ctx.channel(),
							ContainerConstants.ATTR_CLIENTSESSION_COLLECTOR
						);
						if (isSuccess) {
							DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse("success".getBytes());
							channel.writeAndFlush(response);
							if (collector != null) {
								collector.finishSubRequest(end - start);
							}
						} else {
							
							DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse("fail".getBytes());
							channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
							if (collector != null) {
								collector.failSubRequest();
							}
						}
					}
				}
			);

			return true;
		} catch (Exception e) {
			LOG.error("handle sub request for channel={}, request={} error", channel.id(), request != null ? JSONUtil.toJsonStringSilent(request, true) : "NULL SUB REQ", e);
			DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse("fail".getBytes());
			channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
			return false;
		}
	}
}
