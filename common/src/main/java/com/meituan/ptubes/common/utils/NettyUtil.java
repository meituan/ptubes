package com.meituan.ptubes.common.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class NettyUtil {

    public static DefaultFullHttpResponse wrappedFullResponse(byte[] rawData) {
        ByteBuf outData = Unpooled.wrappedBuffer(rawData);
        DefaultFullHttpResponse fullHttpResponse = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.OK,
            outData
        );
        fullHttpResponse.headers()
            .set(
                HttpHeaderNames.CONTENT_LENGTH,
                rawData.length
            );
        return fullHttpResponse;
    }
}
