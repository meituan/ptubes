package com.meituan.ptubes.sdk.reader.processor;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import com.meituan.ptubes.common.utils.JacksonUtil;
import com.meituan.ptubes.common.utils.NettyUtil;
import com.meituan.ptubes.sdk.config.ReaderConnectionConfig;
import com.meituan.ptubes.sdk.model.ServerInfo;
import com.meituan.ptubes.sdk.reader.bean.ClientRequest;
import org.apache.commons.lang3.StringUtils;

public class ServerInfoProcessor implements IProcessor<ReaderConnectionConfig> {

    private static final String EXPECTED_PATH = "/v1/getServerInfo";

    @Override
    public boolean isMatch(ClientRequest clientRequest) {
        return EXPECTED_PATH.equals(clientRequest.getPath());
    }

    @Override
    public ReaderConnectionConfig decode(ClientRequest request) {
        String body = request.getBody()
            .toString(StandardCharsets.UTF_8);
        if (StringUtils.isBlank(body)) {
            return null;
        } else {
            return JacksonUtil.fromJson(
                body,
                ReaderConnectionConfig.class
            );
        }
    }

    @Override
    public boolean process0(
        ChannelHandlerContext ctx,
        ReaderConnectionConfig request
    ) {
        try {
            ServerInfo serverInfo = new ServerInfo();
            serverInfo.setServerStatus(ServerInfo.ServerStatus.AVAILABLE);
            serverInfo.setLoadFactor(50);
            serverInfo.setWriterTaskList(new ArrayList<String>() {{
                add("clientDemo");
            }});
            DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse(JacksonUtil.toJson(serverInfo)
                                                                                 .getBytes());
            ctx.channel()
                .writeAndFlush(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }
}
