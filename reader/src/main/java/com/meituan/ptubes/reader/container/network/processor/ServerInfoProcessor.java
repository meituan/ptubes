package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import com.meituan.ptubes.reader.producer.mysqlreplicator.ReplicatorEventProducer;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.producer.TableConfig;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.manager.ReaderTaskManager;
import com.meituan.ptubes.reader.container.network.request.conn.MySQLSourceConnectionConfig;
import com.meituan.ptubes.reader.container.utils.TypeCastUtil;
import com.meituan.ptubes.reader.monitor.collector.AggregatedClientSessionStatMetricsCollector;
import com.meituan.ptubes.reader.producer.EventProducer;
import com.meituan.ptubes.sdk.config.ReaderConnectionConfig;
import com.meituan.ptubes.sdk.model.DatabaseInfo;
import com.meituan.ptubes.sdk.model.ServerInfo;
import com.meituan.ptubes.sdk.model.ServiceGroupInfo;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class ServerInfoProcessor extends NonVerifiableProcessor<ReaderConnectionConfig> {

    
    private static final Logger LOG = LoggerFactory.getLogger(ServerInfoProcessor.class);
    protected static final String EXPECTED_PATH = "/v1/getServerInfo";

    private final ReaderTaskManager readerTaskManager;
    private final AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector;
    private final ExecutorService executors;

    public ServerInfoProcessor(ReaderTaskManager readerTaskManager, AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector) {
        this.readerTaskManager = readerTaskManager;
        this.aggregatedClientSessionStatMetricsCollector = aggregatedClientSessionStatMetricsCollector;
        this.executors = null;
    }

    public ServerInfoProcessor(ReaderTaskManager readerTaskManager, AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector, ExecutorService executors) {
        this.readerTaskManager = readerTaskManager;
        this.aggregatedClientSessionStatMetricsCollector = aggregatedClientSessionStatMetricsCollector;
        this.executors = executors;
    }

    @Override
    public boolean isMatch(ClientRequest clientRequest) {
        return EXPECTED_PATH.equals(clientRequest.getPath());
    }

    @Override
    public ReaderConnectionConfig decode(ChannelHandlerContext ctx, ClientRequest request) {
        String body = request.getBody().toString(Charset.forName("UTF-8"));
        if (StringUtils.isBlank(body)) {
            return null;
        } else {
            
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
                        Optional<MySQLSourceConnectionConfig> mySQLSourceConnectionConfig = JSONUtil.jsonToSimpleBean(body, MySQLSourceConnectionConfig.class);
                        return mySQLSourceConnectionConfig.isPresent() ? mySQLSourceConnectionConfig.get() : null;
                    default:
                        return null;
                }
            } else {
                return null;
            }
        }
    }

    @Override
    public boolean process0(ChannelHandlerContext ctx, ReaderConnectionConfig request) {
        ServerInfoReqTask task = new ServerInfoReqTask(
            ctx,
            request,
            readerTaskManager,
            aggregatedClientSessionStatMetricsCollector
        );
        if (executors == null) {
            task.run();
        } else {
            executors.submit(task);
        }
        return true;
    }

    private static class ServerInfoReqTask implements Runnable {
        private ChannelHandlerContext ctx;
        private ReaderConnectionConfig request;
        private ReaderTaskManager readerTaskManager;
        private AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector;

        public ServerInfoReqTask(
            ChannelHandlerContext ctx,
            ReaderConnectionConfig request,
            ReaderTaskManager readerTaskManager,
            AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector
        ) {
            this.ctx = ctx;
            this.request = request;
            this.readerTaskManager = readerTaskManager;
            this.aggregatedClientSessionStatMetricsCollector = aggregatedClientSessionStatMetricsCollector;
        }

        @Override
        public void run() {
            Channel channel = ctx.channel();
            if (request == null) {
                LOG.error("parse server info req error, invalid request");
                sendUnavailableMsgAndClose(channel);
                return;
            }

            String readTaskName = request.getServiceGroupInfo().getServiceGroupName();
            String clientTaskName = request.getServiceGroupInfo().getTaskName();
            EventProducer eventProducer = readerTaskManager.getEventProducer(readTaskName);
            // There is no need to lock the producer here, because this is just a client probe, not a real subscription
            if (Objects.isNull(eventProducer) || eventProducer.isRunning() == false) {
                sendUnavailableMsgAndClose(channel);
                LOG.warn("client task {} poll server info, but producer {} is not ready yet", clientTaskName, readTaskName);
                return;
            }
            handleServerInfoRequest(eventProducer, channel);
        }

        private void handleServerInfoRequest(EventProducer producer, Channel channel) {
            // Judgment table, binlog range
            String readerTaskName = request.getServiceGroupInfo().getServiceGroupName();
            // writeTaskName
            String clientTaskName = request.getServiceGroupInfo().getTaskName();
            try {
                Set<DatabaseInfo> databaseInfos = request.getServiceGroupInfo().getDatabaseInfoSet();
                BuffaloCheckpoint checkpoint = request.getBuffaloCheckpoint();
                Map<String, TableConfig> tableConfigMap;
                if (producer instanceof ReplicatorEventProducer) {
                    tableConfigMap = ((ReplicatorEventProducer) producer).getProducerConfig().getTableConfigs();
                }else {
                    tableConfigMap = Collections.emptyMap();
                }
                for (DatabaseInfo databaseInfo : databaseInfos) {
                    String db = databaseInfo.getDatabaseName();
                    for (String tbl : databaseInfo.getTableNames()) {
                        String fullTableName = db + "." + tbl;
                        if (MapUtils.isEmpty(tableConfigMap) || tableConfigMap.containsKey(fullTableName)) {
                            continue;
                        } else {
                            sendUnavailableMsgAndClose(channel);
                            
                            LOG.error("{} send server info req: {}\nclient task {} illegal config: table {} is not exist in readerTask {}",
                                NettyUtil.getChannelIP(ctx.channel()), JSONUtil.toJsonStringSilent(request, true),
                                clientTaskName ,fullTableName, readerTaskName);
                            return;
                        }
                    }
                }

                if (LOG.isDebugEnabled()) {
                    Pair<BinlogInfo, BinlogInfo> range = producer.getBinlogInfoRange();
                    LOG.debug("binlog info range = (min: {} | max: {})", range.getLeft().toString(), range.getRight().toString());
                }
                BuffaloCheckpoint.CheckpointMode checkpointMode = checkpoint.getCheckpointMode();
                switch (checkpointMode) {
                    case NORMAL:
                        BinlogInfo binlogInfoFormCheckpoint = TypeCastUtil.checkpointToBinlogInfo(checkpoint);
                        StorageConstant.StorageRangeCheckResult storageRangeCheckResult = producer.isInStorageRange(binlogInfoFormCheckpoint);
                        if (StorageConstant.StorageRangeCheckResult.IN_RANGE.equals(storageRangeCheckResult)) {
                            ServerInfo serverInfo = new ServerInfo();
                            serverInfo.setServerStatus(ServerInfo.ServerStatus.AVAILABLE);
                            serverInfo.setLoadFactor(0);
                            serverInfo.setWriterTaskList(aggregatedClientSessionStatMetricsCollector.clientSessionTaskName());
                            DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse(JSONUtil.toJsonString(
                                serverInfo,
                                false
                            ).getBytes());
                            channel.writeAndFlush(response)/*.addListener(ChannelFutureListener.CLOSE)*/;
                        } else if (StorageConstant.StorageRangeCheckResult.LESS_THAN_MIN.equals(storageRangeCheckResult)) {
                            sendUnavailableMsgAndClose(channel);
                            LOG.warn("{} send server info req: {}\nclient task {} illegal config: checkpoint less than earliest binlog range in readerTask {}",
                                NettyUtil.getChannelIP(ctx.channel()), JSONUtil.toJsonStringSilent(request, true),
                                clientTaskName, readerTaskName);
                        } else if (StorageConstant.StorageRangeCheckResult.GREATER_THAN_MAX.equals(storageRangeCheckResult)) {
                            sendUnavailableMsgAndClose(channel);
                            LOG.warn("{} send server info req: {}\nclient task {} illegal config: checkpoint greater than latest binlog range in readerTask {}",
                                NettyUtil.getChannelIP(ctx.channel()), JSONUtil.toJsonStringSilent(request, true),
                                clientTaskName, readerTaskName);
                        } else {
                            // it's better not to reach here
                            sendUnavailableMsgAndClose(channel);
                            LOG.warn("{} send server info req: {}\nclient task {} call unready readerTask {}",
                                NettyUtil.getChannelIP(ctx.channel()), JSONUtil.toJsonStringSilent(request, true),
                                clientTaskName, readerTaskName);
                        }
                        break;
                    case LATEST:
                    case EARLIEST:
                        ServerInfo serverInfo = new ServerInfo();
                        serverInfo.setServerStatus(ServerInfo.ServerStatus.AVAILABLE);
                        serverInfo.setLoadFactor(0);
                        serverInfo.setWriterTaskList(aggregatedClientSessionStatMetricsCollector.clientSessionTaskName());
                        DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse(JSONUtil.toJsonString(
                            serverInfo,
                            false
                        ).getBytes());
                        channel.writeAndFlush(response);
                        break;
                    default:
                        LOG.error("client task {} has unknown checkpoint mode {}", clientTaskName, checkpointMode);
                        sendUnavailableMsgAndClose(channel);
                        break;
                }
            } catch (Exception e) {
                LOG.error("handle client {} serverInfo req error", clientTaskName, e);
                sendUnavailableMsgAndClose(channel);
            }
        }

        private static final String UNAVAILABLE_SERVER_INFO_MSG;
        static {
            ServerInfo serverInfo = new ServerInfo();
            serverInfo.setServerStatus(ServerInfo.ServerStatus.UNAVAILABLE);
            UNAVAILABLE_SERVER_INFO_MSG = JSONUtil.toJsonStringSilent(
                serverInfo,
                false
            );
        }
        private void sendUnavailableMsgAndClose(Channel channel) {
            ServerInfo serverInfo = new ServerInfo();
            serverInfo.setServerStatus(ServerInfo.ServerStatus.UNAVAILABLE);
            DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse(UNAVAILABLE_SERVER_INFO_MSG.getBytes());
            channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }

    }

}
