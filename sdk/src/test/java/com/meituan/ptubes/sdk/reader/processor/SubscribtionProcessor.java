package com.meituan.ptubes.sdk.reader.processor;

import com.meituan.ptubes.sdk.reader.bean.SubRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import java.nio.charset.StandardCharsets;
import com.meituan.ptubes.common.utils.JacksonUtil;
import com.meituan.ptubes.common.utils.NettyUtil;
import com.meituan.ptubes.sdk.reader.bean.ClientRequest;

public class SubscribtionProcessor implements IProcessor<SubRequest> {

    private static final String EXPECTED_PATH = "/v1/subscribe";

    @Override
    public boolean isMatch(ClientRequest clientRequest) {
        return EXPECTED_PATH.equals(clientRequest.getPath());
    }

    @Override
    public SubRequest decode(ClientRequest request) {
        String bodyStr = request.getBody()
            .toString(StandardCharsets.UTF_8);
        return JacksonUtil.fromJson(
            bodyStr,
            SubRequest.class
        );
    }

    @Override
    public boolean process0(
        ChannelHandlerContext ctx,
        SubRequest request
    ) {
        DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse("success".getBytes());
        ctx.channel()
            .writeAndFlush(response);
        return true;
    }
}
