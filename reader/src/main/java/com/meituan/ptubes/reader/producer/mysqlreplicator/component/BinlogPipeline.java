package com.meituan.ptubes.reader.producer.mysqlreplicator.component;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import com.meituan.ptubes.common.utils.IPUtil;
import com.meituan.ptubes.reader.container.common.config.handler.AbstractMaxBinlogInfoReaderWriter;
import com.meituan.ptubes.reader.container.common.config.producer.RdsConfig;
import com.meituan.ptubes.reader.container.common.config.service.ProducerConfigService;
import com.meituan.ptubes.reader.container.common.vo.MySQLMaxBinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.producer.mysqlreplicator.ReplicatorEventProducer;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.BinlogData;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.BinlogDataFactory;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.BinlogDataHandlerThreadFactory;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.PipelineExceptionHandler;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventListener;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MathUtil;
import com.meituan.ptubes.reader.producer.sequencer.Sequencer;
import com.meituan.ptubes.reader.schema.provider.ReadTaskSchemaProviderV2;
import com.meituan.ptubes.reader.storage.channel.WriteChannel;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public class BinlogPipeline implements BinlogEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogPipeline.class);
    private static final AtomicInteger THREAD_ID_GENERATOR = new AtomicInteger(1);

    protected final ReplicatorEventProducer eventProducer;
    protected final ProducerConfigService producerConfigService;
    protected final WriteChannel writeChannel;
    protected final AbstractMaxBinlogInfoReaderWriter maxBinlogInfoReaderWriter;
    protected final ReadTaskSchemaProviderV2 schemaProvider;
    protected final Sequencer sequencer;

    protected final MySQLMaxBinlogInfo startAtMaxBinlogInfo;
    protected final short changeId;
    protected final RdsConfig rdsConfig;
    protected final long fromIp;
    protected final ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean shutdownRequest = new AtomicBoolean(false);
    private final int binlogDataBufferSize;
    private volatile BinlogContextParser binlogContextParser;
    protected volatile Disruptor<BinlogData> binlogDataDisruptor;
    protected volatile RingBuffer<BinlogData> binlogDataBuffer;
    private volatile long prevEventReceiveTimeMS = System.currentTimeMillis();

    private EventHandlerGroup rowParserGroup;
    private EventHandlerGroup writerGroup;

    public BinlogPipeline(ReplicatorEventProducer eventProducer, ProducerConfigService producerConfigService,
        WriteChannel writeChannel, AbstractMaxBinlogInfoReaderWriter maxBinlogInfoReaderWriter, ReadTaskSchemaProviderV2 schemaProvider,
        int binlogDataBufferSize, MySQLMaxBinlogInfo maxBinlogInfo, short changeId, ReaderTaskStatMetricsCollector statMetricsCollector,
        Sequencer sequencer) {
        this.eventProducer = eventProducer;
        this.producerConfigService = producerConfigService;
        this.writeChannel = writeChannel;
        this.maxBinlogInfoReaderWriter = maxBinlogInfoReaderWriter;
        this.schemaProvider = schemaProvider;
        this.binlogDataBufferSize = MathUtil.normalizeBufferSize(binlogDataBufferSize, 1, 4096);
        this.startAtMaxBinlogInfo = maxBinlogInfo;
        this.sequencer = sequencer;

        this.changeId = changeId;
        this.rdsConfig = producerConfigService.getConfig().getRdsConfig();
        this.fromIp = IPUtil.ipToLong(rdsConfig.getIp());
        this.readerTaskStatMetricsCollector = statMetricsCollector;
    }

    public long stageBufferRemaining() {
        return binlogDataDisruptor.getBufferSize() - (binlogDataDisruptor.getCursor() - writerGroup.asSequenceBarrier().getCursor()); //binlogDataBuffer.remainingCapacity();
    }
    public long rowParserGroupBacklog() {
        return binlogDataDisruptor.getCursor() - rowParserGroup.asSequenceBarrier().getCursor();
    }
    public long writerGroupBacklog() {
        return rowParserGroup.asSequenceBarrier().getCursor() - writerGroup.asSequenceBarrier().getCursor();
    }

    protected boolean isShutdownRequested() {
        return shutdownRequest.get();
    }

    protected void markShutdownRequest() {
        shutdownRequest.set(true);
    }

    public void prepare(){
        //This is just to initialize some variables, there is no problem even if repeated execution
        this.binlogDataDisruptor = new Disruptor<>(BinlogDataFactory.INSTANCE, binlogDataBufferSize,
                new BinlogDataHandlerThreadFactory(eventProducer.getReaderTaskName()),
                ProducerType.SINGLE, new BlockingWaitStrategy());
        this.binlogDataBuffer = binlogDataDisruptor.getRingBuffer();

        RowParser[] rowParsers = new RowParser[12];
        for (int i = 0; i < rowParsers.length; ++i) {
            rowParsers[i] = new RowParser(this);
        }
        BinlogWriter binlogWriter = new BinlogWriter(this);

        this.rowParserGroup = binlogDataDisruptor.handleEventsWithWorkerPool(rowParsers);
        this.writerGroup =  rowParserGroup.thenHandleEventsWithWorkerPool(binlogWriter);
        binlogDataDisruptor.setDefaultExceptionHandler(new PipelineExceptionHandler());
        this.binlogContextParser = new BinlogContextParser(this, 512);
        LOG.info("prepare {} binlog pipeline resource success", eventProducer.getReaderTaskName());
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            binlogDataDisruptor.start();
            binlogContextParser.start();
            LOG.info("start {} binlog pipeline startup [true]", eventProducer.getReaderTaskName());
        }
    }

    public void stop() {
        running.set(false);
        LOG.info("try to stop {} binlog pipeline", eventProducer.getReaderTaskName());
        markShutdownRequest();
        binlogContextParser.shutdown();
        binlogDataDisruptor.shutdown();
        LOG.info("{} binlog pipeline shutdown [true]", eventProducer.getReaderTaskName());
    }

    public void stop(long timeout, TimeUnit timeUnit) throws TimeoutException {
        running.set(false);
        LOG.info("try to stop {} disruptor topology", eventProducer.getReaderTaskName());
        markShutdownRequest();
        binlogContextParser.shutdown();
        boolean contextParserShutdownRes = binlogContextParser.awaitShutdownUninteruptibly(timeout, timeUnit);
        LOG.info("{} context parser shutdown [{}]", eventProducer.getReaderTaskName(), contextParserShutdownRes);
        binlogDataDisruptor.shutdown(timeout, timeUnit);
        LOG.info("{} binlog pipeline shutdown [true]", eventProducer.getReaderTaskName());
    }

    public void pause() throws InterruptedException {
        LOG.info("try to pause {} binlog pipeline", eventProducer.getReaderTaskName());
        binlogContextParser.pause();
        LOG.info("{} binlog pipeline pause [true]", eventProducer.getReaderTaskName());
    }
    public void unpause() throws InterruptedException {
        LOG.info("try to unpause {} binlog pipeline", eventProducer.getReaderTaskName());
        binlogContextParser.unpause();
        LOG.info("{} binlog pipeline unpause [true]", eventProducer.getReaderTaskName());
    }

    public long millisSinceLastEvent() {
        return System.currentTimeMillis() - prevEventReceiveTimeMS;
    }
    public boolean isAlive() {
        return running.get() && binlogContextParser.isAlive();
    }

    @Override public void onEvents(BinlogEventV4 event) {
        while (!isShutdownRequested()) {
            try {
                // do you need a timer coroutine?
                prevEventReceiveTimeMS = System.currentTimeMillis();
                binlogContextParser.onEvent(event);
                break;
            } catch (InterruptedException ie) {
                LOG.error("interrupt publishing event to context parser, action will be retried until shutdown", ie);
            }
        }
    }
}
