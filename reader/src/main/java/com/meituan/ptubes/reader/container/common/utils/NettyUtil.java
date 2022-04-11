package com.meituan.ptubes.reader.container.common.utils;

import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.stream.ChunkedStream;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import com.meituan.ptubes.reader.container.common.vo.Tuple2;

public class NettyUtil {

	public static byte[] bytebufToByteArray(ByteBuf byteBuf, boolean shouldChangeReaderIndex) {
		byte[] ans = new byte[byteBuf.readableBytes()];

		if (shouldChangeReaderIndex) {
			byteBuf.readBytes(ans);
		} else {
			int arrayOffset = byteBuf.arrayOffset();
			byteBuf.getBytes(arrayOffset, ans);
		}
		return ans;
	}

	public static <T> void setChannelAttribute(Channel channel, String key, T value) {
		AttributeKey<T> attrKey = AttributeKey.valueOf(key);
		Attribute<T> attrValue = channel.attr(attrKey);
		attrValue.set(value);
	}

	public static <T> T getChannelAttribute(Channel channel, String key) {
		AttributeKey<T> attrKey = AttributeKey.valueOf(key);
		if (channel.hasAttr(attrKey)) {
			return channel.attr(attrKey).get();
		} else {
			return null;
		}
	}

	/**
	 * Pay attention to the number of references to data. If there is a release outside, remember to duplicate it
	 * @param data
	 * @return
	 */
	public static DefaultFullHttpResponse wrappedFullResponse(ByteBuf data) {
		DefaultFullHttpResponse fullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, data);
		fullHttpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, data.readableBytes());
		return fullHttpResponse;
	}

	public static DefaultFullHttpResponse wrappedFullResponse(byte[] rawData) {
		ByteBuf outData = Unpooled.wrappedBuffer(rawData);
		DefaultFullHttpResponse fullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, outData);
		fullHttpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, rawData.length);
		return fullHttpResponse;
	}

	private static DefaultHttpResponse wrappedChunkedResponseHeader() {
		DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		httpResponse.headers().set(HttpHeaderNames.TRANSFER_ENCODING, "chunked");
		return httpResponse;
	}
	private static HttpChunkedInput wrappedChunkedResponseContent(byte[] rawData) {
		HttpChunkedInput chunkedInput = new HttpChunkedInput(new ChunkedStream(new ByteArrayInputStream(rawData), ContainerConstants.MAX_CHUNK_SIZE));
		return chunkedInput;
	}
	public static Tuple2<DefaultHttpResponse, HttpChunkedInput> wrappedChunkedResponse(byte[] rawData) {
		return new Tuple2(wrappedChunkedResponseHeader(), wrappedChunkedResponseContent(rawData));
	}

	public static String getChannelIP(Channel channel) {
		try {
			InetSocketAddress socketAddress = (InetSocketAddress) channel.remoteAddress();
			String ip = socketAddress.getAddress().getHostAddress();
			return ip;
		} catch (Exception e) {
			return "0.0.0.0";
		}
	}

}
