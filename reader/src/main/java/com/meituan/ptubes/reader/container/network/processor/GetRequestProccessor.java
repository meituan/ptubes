package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import com.meituan.ptubes.reader.container.network.request.GetRequest;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import java.nio.charset.Charset;
import java.util.Optional;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.container.manager.SessionManager;
import com.meituan.ptubes.reader.container.network.connections.AbstractClientSession;
import com.meituan.ptubes.reader.monitor.collector.ClientSessionStatMetricsCollector;
import com.meituan.ptubes.sdk.model.DataRequest;

public class GetRequestProccessor extends VerifiableProcessor<GetRequest> {

	private static final Logger LOG = LoggerFactory.getLogger(GetRequestProccessor.class);

	protected static final String EXPECTED_PATH = "/v1/fetchMessages";

	private final SessionManager sessionManager;

	public GetRequestProccessor(SessionManager sessionManager) {
		this.sessionManager = sessionManager;
	}

	@Override
	public boolean isMatch(ClientRequest clientRequest) {
		return EXPECTED_PATH.equals(clientRequest.getPath());
	}

	@Override
	protected GetRequest decode(ChannelHandlerContext ctx, ClientRequest request) {
		String bodyStr = request.getBody().toString(Charset.forName("UTF-8"));
		Optional<DataRequest> dataRequestOptional = JSONUtil.jsonToSimpleBean(bodyStr, DataRequest.class);
		if (dataRequestOptional.isPresent() == false) {
			LOG.warn("parse GetRequest {} fail", bodyStr);
			return null;
		} else {
			DataRequest dataRequest = dataRequestOptional.get();
			return new GetRequest(dataRequest.getMaxByteSize(), dataRequest.getBatchSize(), dataRequest.getTimeoutMs());
		}
	}

	@Override
	protected boolean process0(ChannelHandlerContext ctx, GetRequest request) {
		Channel channel = ctx.channel();
		try {
			if (request == null) {
				DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse("fail".getBytes());
				channel.writeAndFlush(response);
				return false;
				
			}

			String clientId = NettyUtil.getChannelAttribute(channel, ContainerConstants.ATTR_CLIENTID);
			if (null == clientId) {
				try {
					LOG.warn("server receive getRequest from channel {} which has no clientId, channel close", channel.id().asShortText());
				} finally {
					channel.close();
				}
				return false;
			} else {
				
				AbstractClientSession session = sessionManager.getSession(clientId);
				if (null == session) {
					LOG.warn("no matched session for clientId {}, channel close");
					DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse("fail".getBytes());
					channel.writeAndFlush(response)
						.addListener(ChannelFutureListener.CLOSE);
					return false;
				} else {
					
					LOG.debug("read req channel {} session {}", channel.id(), clientId);
					if (session.offerGetRequest(request) == false) {
						
						LOG.warn(
							"get request queue is full for clientId {}, skip this get req",
							clientId
						);
						DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse("fail".getBytes());
						channel.writeAndFlush(response);
					} else {
						// through consumption com.meituan.ptubes.reader.network.connections.SessionTransmitter.getRequests
						// Data returned by the transmitter thread
						LOG.debug("offer get request {}", request);
					}
					return true;
				}
			}
		} catch (Exception e) {
			DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse("fail".getBytes());
			channel.writeAndFlush(response);
			LOG.error("handle get request for channel={} error", channel.id(), e);
			return false;
		}
	}

	@Override
	public boolean process(ChannelHandlerContext ctx, ClientRequest request) {
		long start = System.currentTimeMillis();
		boolean processRes = super.process(ctx, request);
		long end = System.currentTimeMillis();

		ClientSessionStatMetricsCollector collector = NettyUtil.getChannelAttribute(
			ctx.channel(),
			ContainerConstants.ATTR_CLIENTSESSION_COLLECTOR
		);
		if (collector != null) {
			if (processRes) {
				collector.finishGetRequest(end - start);
				return true;
			} else {
				collector.failGetRequest();
				return false;
			}
		} else {
			// never reach here
			if (LOG.isDebugEnabled()) {
				LOG.warn("clientSession collector of channel {} is not exist", ctx.channel().id().asShortText());
			}
		}
		return processRes;
	}

}
