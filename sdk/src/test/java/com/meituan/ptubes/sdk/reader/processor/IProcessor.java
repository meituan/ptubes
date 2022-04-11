package com.meituan.ptubes.sdk.reader.processor;

import com.meituan.ptubes.sdk.reader.bean.ClientRequest;
import io.netty.channel.ChannelHandlerContext;

public interface IProcessor<T> {

    boolean isMatch(ClientRequest clientRequest);

    T decode(ClientRequest request);

    default boolean process(
        ChannelHandlerContext ctx,
        ClientRequest request
    ) {
        T req = decode(request);
        return process0(
            ctx,
            req
        );
    }

    boolean process0(
        ChannelHandlerContext ctx,
        T request
    );
}
