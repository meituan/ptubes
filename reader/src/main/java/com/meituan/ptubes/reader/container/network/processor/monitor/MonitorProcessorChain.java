package com.meituan.ptubes.reader.container.network.processor.monitor;

import com.meituan.ptubes.reader.container.network.processor.DefaultProcessor;
import com.meituan.ptubes.reader.container.network.processor.IProcessor;
import com.meituan.ptubes.reader.container.network.processor.IProcessorChain;
import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import com.meituan.ptubes.reader.container.manager.CollectorManager;
import com.meituan.ptubes.reader.container.manager.ReaderTaskManager;

public class MonitorProcessorChain implements IProcessorChain {

    private final CollectorManager collectorManager;
    private List<IProcessor> processors = new ArrayList<>();
    private final ReaderTaskManager readerTaskManager; // just for read

    public MonitorProcessorChain(CollectorManager collectorManager, ReaderTaskManager readerTaskManager) {
        this.collectorManager = collectorManager;
        this.readerTaskManager = readerTaskManager;
        processors.add(new BasicInfoProcessor(this.collectorManager.getApplicationStatMetricsCollector()));
        processors.add(new ContainerInfoProcessor(this.collectorManager.getContainerStatMetricsCollector()));
        processors.add(new ClientSessionInfoProcessor(this.collectorManager.getAggregatedClientSessionStatMetricsCollector()));
        processors.add(new ReaderTaskInfoProcessor(this.collectorManager.getAggregatedReadTaskStatMetricsCollector()));
        processors.add(new RuntimeConfInfoProcessor(this.readerTaskManager));
        processors.add(new DefaultProcessor());
    }

    @Override
    public void process(ChannelHandlerContext ctx, ClientRequest clientRequest) {
        for (IProcessor processor : this.processors) {
            if (processor.isMatch(clientRequest)) {
                processor.process(ctx, clientRequest);
            } else {
                continue;
            }
        }
    }

}
