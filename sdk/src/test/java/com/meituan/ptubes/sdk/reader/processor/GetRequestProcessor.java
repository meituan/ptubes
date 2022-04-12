package com.meituan.ptubes.sdk.reader.processor;

import com.meituan.ptubes.sdk.reader.bean.GetRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import java.nio.charset.StandardCharsets;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.JacksonUtil;
import com.meituan.ptubes.common.utils.NettyUtil;
import com.meituan.ptubes.sdk.model.DataRequest;
import com.meituan.ptubes.sdk.protocol.RdsPacket;
import com.meituan.ptubes.sdk.reader.bean.ClientRequest;

public class GetRequestProcessor implements IProcessor<GetRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(GetRequestProcessor.class);
    private static final String EXPECTED_PATH = "/v1/fetchMessages";

    @Override
    public boolean isMatch(ClientRequest clientRequest) {
        return EXPECTED_PATH.equals(clientRequest.getPath());
    }

    @Override
    public GetRequest decode(ClientRequest request) {
        String bodyStr = request.getBody()
            .toString(StandardCharsets.UTF_8);
        DataRequest dataRequest = JacksonUtil.fromJson(
            bodyStr,
            DataRequest.class
        );
        return new GetRequest(
            dataRequest.getMaxByteSize(),
            dataRequest.getBatchSize(),
            dataRequest.getTimeoutMs()
        );
    }

    @Override
    public boolean process0(
        ChannelHandlerContext ctx,
        GetRequest request
    ) {
        RdsPacket.Checkpoint checkpoint = RdsPacket.Checkpoint.newBuilder()
            .setUuid("uuid")
            .setTransactionId(1)
            .setEventIndex(2)
            .setServerId(3)
            .setBinlogFile(4)
            .setBinlogOffset(5)
            .setTimestamp(System.currentTimeMillis())
            .setVersionTs(6)
            .build();

        String column = "id";
        RdsPacket.Column beforeColumn = RdsPacket.Column.newBuilder()
            .setName(column)
            .setValue("123")
            .build();
        RdsPacket.Column afterColumn = RdsPacket.Column.newBuilder()
            .setName(column)
            .setValue("456")
            .build();

        RdsPacket.RowData rowData = RdsPacket.RowData.newBuilder()
            .putBeforeColumns(
                column,
                beforeColumn
            )
            .putAfterColumns(
                column,
                afterColumn
            )
            .build();

        RdsPacket.RdsHeader rdsCdcHeader = RdsPacket.RdsHeader.newBuilder()
            .setCheckpoint(checkpoint)
            .build();

        RdsPacket.RdsEvent rdsEvent = RdsPacket.RdsEvent.newBuilder()
            .setHeader(rdsCdcHeader)
            .setRowData(rowData)
            .build();

        RdsPacket.RdsMessage rdsMessage = RdsPacket.RdsMessage.newBuilder()
            .addMessages(
                rdsEvent.toByteString()
            )
            .build();
        RdsPacket.RdsPartitionedMessage rdsPartitionedMessage = RdsPacket.RdsPartitionedMessage.newBuilder()
            .putPartitionMessages(
                1,
                rdsMessage
            )
            .build();

        RdsPacket.RdsEnvelope rdsEnvelope = RdsPacket.RdsEnvelope.newBuilder()
            .setMessage(rdsPartitionedMessage.toByteString())
            .build();

        DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse(rdsEnvelope.toByteArray());
        ctx.channel()
            .writeAndFlush(response);
        return true;
    }
}
