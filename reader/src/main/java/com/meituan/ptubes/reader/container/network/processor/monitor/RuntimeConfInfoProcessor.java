package com.meituan.ptubes.reader.container.network.processor.monitor;

import com.meituan.ptubes.reader.container.network.processor.NonVerifiableProcessor;
import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.container.manager.ReaderTaskManager;
import com.meituan.ptubes.reader.container.network.request.monitor.RuntimeConfRequest;
import com.meituan.ptubes.reader.monitor.vo.ReaderRuntimeInfo;
import org.apache.commons.collections.CollectionUtils;

public class RuntimeConfInfoProcessor extends NonVerifiableProcessor<RuntimeConfRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(RuntimeConfInfoProcessor.class);

    private static final String EXPECTED_PATH = "/v1/readerTaskRuntimeConf";

    ReaderTaskManager readerTaskManager;

    public RuntimeConfInfoProcessor(ReaderTaskManager readerTaskManager) {
        this.readerTaskManager = readerTaskManager;
    }

    @Override
    public boolean isMatch(ClientRequest clientRequest) {
        return EXPECTED_PATH.equals(clientRequest.getPath());
    }

    @Override
    protected RuntimeConfRequest decode(ChannelHandlerContext ctx, ClientRequest request) {
        LOG.info("[uri={}][request={}]", EXPECTED_PATH, request);
        Map<String, List<String>> params = request.getPathVariables();
        RuntimeConfRequest runtimeConfRequest = new RuntimeConfRequest();
        runtimeConfRequest.setTaskName(request.getPathVariables().get("taskName"));
        runtimeConfRequest.setAllTask(!CollectionUtils.isEmpty(params.get("allTask")) && Boolean.parseBoolean(params.get("allTask").get(0)));
        return runtimeConfRequest;
    }

    @Override
    protected boolean process0(ChannelHandlerContext ctx, RuntimeConfRequest request) {
        DefaultFullHttpResponse response;
        try {
            response = NettyUtil.wrappedFullResponse(JSONUtil.toJsonString(
                    fetchRuntimeInfos(request),
                    false
            ).getBytes());
        } catch (IOException ioe) {
            LOG.warn("transfer reader task runtime conf to json string error", ioe);
            response = NettyUtil.wrappedFullResponse("[]".getBytes());
        }

        ctx.writeAndFlush(response);
        return true;
    }

    private List<ReaderRuntimeInfo> fetchRuntimeInfos(RuntimeConfRequest request) {
        List<ReaderRuntimeInfo> result = new ArrayList<>();
        List<String> targetTasks = null;
        if (request.getAllTask()) {
            targetTasks= new ArrayList<>(readerTaskManager.getCurrentReadTaskSet());
        } else {
            targetTasks = request.getTaskName();
        }

        if (CollectionUtils.isEmpty(targetTasks)) {
            return result;
        }
        for (String taskName : targetTasks) {
            ReaderRuntimeInfo readerRuntimeInfo = readerTaskManager.fetchRuntimeConf(taskName);
            if (null != readerRuntimeInfo) {
                result.add(readerRuntimeInfo);
            }
        }
        return result;

    }
}
