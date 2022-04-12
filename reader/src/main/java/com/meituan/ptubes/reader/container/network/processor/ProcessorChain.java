package com.meituan.ptubes.reader.container.network.processor;

import com.meituan.ptubes.reader.container.network.request.ClientRequest;
import io.netty.channel.ChannelHandlerContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.meituan.ptubes.reader.container.manager.CollectorManager;
import com.meituan.ptubes.reader.container.manager.ReaderTaskManager;
import com.meituan.ptubes.reader.container.manager.SessionManager;

/**
 * Chain of responsibility, why not put the processor in the pipeline like puma
 * 1.simpleInboundHandler will judge whether the generic type matches the type passed later, this judgment is lossy
 * 2. The same request can flexibly process different parts of the data
 */
public class ProcessorChain implements IProcessorChain {

	private ExecutorService processorThreadPool =
		new ThreadPoolExecutor(
			1,
			Math.max(
				2,
				Runtime.getRuntime()
					.availableProcessors() * 2
			),
			30,
			TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>(2048),
			new ThreadFactory() {
				private final String threadNamePrefix = "processor-executor";
				private final AtomicInteger tid = new AtomicInteger(1);

				@Override
				public Thread newThread(Runnable r) {
					return new Thread(
						r,
						threadNamePrefix +
							tid.getAndIncrement()
					);
				}
			},
			
			new ThreadPoolExecutor.CallerRunsPolicy()
		);

	
	private final SessionManager sessionManager;
	private final ReaderTaskManager readerTaskManager;
	private final CollectorManager collectorManager;

	private final Map<String, IProcessor> processorMap = new HashMap<>();
	private final IProcessor defaultProcessor = new DefaultProcessor();

	public ProcessorChain(SessionManager sessionManager, ReaderTaskManager readerTaskManager, CollectorManager collectorManager) {
		this.sessionManager = sessionManager;
		this.readerTaskManager = readerTaskManager;
		this.collectorManager = collectorManager;

		this.processorMap.put(GetRequestProccessor.EXPECTED_PATH, new GetRequestProccessor(this.sessionManager));
		this.processorMap.put(SubscribtionProcessor.EXPECTED_PATH, new SubscribtionProcessor(this.sessionManager, this.readerTaskManager));
		this.processorMap.put(KeepAliveProcessor.EXPECTED_PATH, new KeepAliveProcessor());
		this.processorMap.put(ServerInfoProcessor.EXPECTED_PATH, new ServerInfoProcessor(this.readerTaskManager, this.collectorManager.getAggregatedClientSessionStatMetricsCollector(), processorThreadPool));
		this.processorMap.put(DumpPointAdjustProcessor.EXPECTED_PATH, new DumpPointAdjustProcessor(this.readerTaskManager, this.sessionManager));
	}

	@Override
	public void process(ChannelHandlerContext ctx, ClientRequest clientRequest) {
		IProcessor processor = processorMap.get(clientRequest.getPath());
		if (Objects.nonNull(processor)) {
			processor.process(ctx, clientRequest);
		} else {
			defaultProcessor.process(ctx, clientRequest);
		}
	}

}
