package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import com.meituan.ptubes.reader.container.network.request.DumpPointAdjustmentRequest;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.Charset;
import java.util.Optional;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.container.manager.ReaderTaskManager;
import com.meituan.ptubes.reader.container.manager.SessionManager;
import com.meituan.ptubes.reader.container.network.response.CommonResponse;
import com.meituan.ptubes.reader.producer.EventProducer;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.reader.storage.mem.MemStorageFactory;
import org.apache.commons.lang3.StringUtils;

public class DumpPointAdjustProcessor extends NonVerifiableProcessor<DumpPointAdjustmentRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(DumpPointAdjustProcessor.class);
    protected static final String EXPECTED_PATH = "/v1/adjustDumpPoint";

    public final ReaderTaskManager readerTaskManager;
    public final SessionManager sessionManager;

    public DumpPointAdjustProcessor(ReaderTaskManager readerTaskManager, SessionManager sessionManager) {
        this.readerTaskManager = readerTaskManager;
        this.sessionManager = sessionManager;
    }

    @Override
    public boolean isMatch(ClientRequest clientRequest) {
        return EXPECTED_PATH.equals(clientRequest.getPath());
    }

    @Override
    public DumpPointAdjustmentRequest decode(ChannelHandlerContext ctx, ClientRequest request) {
        String body = request.getBody().toString(Charset.forName("UTF-8"));
        LOG.debug("get dump point adjust command: {}", body);
        if (StringUtils.isBlank(body)) {
            return null;
        } else {
            Optional<DumpPointAdjustmentRequest> command = JSONUtil.jsonToSimpleBean(
                body,
                DumpPointAdjustmentRequest.class
            );
            if (command.isPresent()) {
                return command.get();
            } else {
                return null;
            }
        }
    }

    private boolean validateRequest(DumpPointAdjustmentRequest dumpPointAdjustmentRequest, SourceType sourceType) {
        boolean res = true;
        switch (dumpPointAdjustmentRequest.getStrategy()) {
            case TIMESTAMP:
                if (dumpPointAdjustmentRequest.getTimestamp() <= 0) {
                    res = false;
                }
                break;
            case BINLOG_POSITION:
                if (dumpPointAdjustmentRequest.getBinlogNum() <= 0 || dumpPointAdjustmentRequest.getBinlogNum() < 0) {
                    res = false;
                }
                break;
            case FREEZE:
            default:
                res = true;
                break;
        }
        return res;
    }

    private void sendResponse(ChannelHandlerContext ctx, CommonResponse response) {
        ctx.channel()
            .writeAndFlush(NettyUtil.wrappedFullResponse(JSONUtil.toJsonStringSilent(
                response,
                false
            ).getBytes()))
            .addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public boolean process0(ChannelHandlerContext ctx, DumpPointAdjustmentRequest request) {
        CommonResponse response = new CommonResponse();
        if (request == null) {
            response.fail("fail to parse dump point adjust request");
            sendResponse(ctx, response);
            return false;
        }

        
        String readerTaskName = request.getReaderTask();
        EventProducer producer = readerTaskManager.getEventProducer(readerTaskName);
        if (producer != null) {
            if (validateRequest(request, producer.getSourceType()) == false) {
                response.fail("invalid request arguments");
                sendResponse(ctx, response);
                return false;
            }

            try {
                synchronized (producer) {
                    if (producer.isRunning() == false) {
                        response.fail("producer of readerTask " + readerTaskName + " is already closed");
                        sendResponse(ctx, response);
                        return false;
                    } else {
                        producer.shutdown();

                        // But if it is not locked, the producer state may be modified by other instructions in the middle, which can be discussed below
                        sessionManager.deregisterSessionsInSync(readerTaskName);
                        FileSystem.backupStorageDir(readerTaskName);

                        // Explicitly close MemStorage to prevent leaks (external references to old MemStorage may throw exceptions)
                        MemStorageFactory.getInstance().forceRecycleMemStorage(readerTaskName);
                        switch (request.getStrategy()) {
                            case FREEZE:
                                producer.start();
                                break;
                            case TIMESTAMP:
                                
                                long startTime = request.getTimestamp();
                                producer.start(startTime);
                                break;
                            case BINLOG_POSITION:
                                producer.start(
                                    request.getBinlogNum(),
                                    request.getBinlogOffset()
                                );
                                break;
                            default:
                        }
                        sendResponse(ctx, response);
                        return true;
                    }
                }
            } catch (Exception e) {
                
                response.fail("adjust dump point error: " + e.getMessage());
                sendResponse(ctx, response);
                return false;
            }
        } else {
            response.fail("producer of readerTask " + readerTaskName + " is not exist or closed already");
            sendResponse(ctx, response);
            return false;
        }

    }

}
