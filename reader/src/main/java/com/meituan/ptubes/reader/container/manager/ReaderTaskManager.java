package com.meituan.ptubes.reader.container.manager;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.reader.container.ReaderTask;
import com.meituan.ptubes.reader.container.common.config.ConfigService;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerBaseConfig;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig;
import com.meituan.ptubes.reader.container.common.config.respository.MySQLConfigRespository;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.config.MySQLConfigRespositoryBuilder;
import com.meituan.ptubes.reader.container.config.ProducerBaseConfigBuilder;
import com.meituan.ptubes.reader.monitor.collector.AggregatedReadTaskStatMetricsCollector;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.monitor.vo.ReaderRuntimeInfo;
import com.meituan.ptubes.reader.producer.EventProducer;
import com.meituan.ptubes.reader.producer.mysqlreplicator.ReplicatorEventProducer;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.meituan.ptubes.reader.container.common.AbstractLifeCycle;

/**
 * Task Manager, which contains a collection of tasks that get data from the source db.
 */
public class ReaderTaskManager extends AbstractLifeCycle {

	private static final Logger LOG = LoggerFactory.getLogger(ReaderTaskManager.class);

    // service group unique id map or
    private final Map<String, ReaderTask> readerTaskMap;
	private final SessionManager sessionManager;
	private final AggregatedReadTaskStatMetricsCollector aggregatedReadTaskStatMetricsCollector;

	public ReaderTaskManager(SessionManager sessionManager, AggregatedReadTaskStatMetricsCollector aggregatedReadTaskStatMetricsCollector) {
		this.readerTaskMap = new ConcurrentHashMap<>();
		this.sessionManager = sessionManager;
		this.aggregatedReadTaskStatMetricsCollector = aggregatedReadTaskStatMetricsCollector;
	}

	// Manager dimension operation lock manager
	@Override
	public synchronized boolean start() {
		return super.start();
	}

	// Manager dimension operation lock manager
	@Override
	public synchronized boolean stop() {
		boolean res = super.stop();

		if (res) {
			// concurrentHashMap
			Iterator<String> readerTaskIterator = readerTaskMap.keySet().iterator();
			while (readerTaskIterator.hasNext()) {
				String readerTaskName = readerTaskIterator.next();
				// may be removed
				EventProducer producer = readerTaskMap.get(readerTaskName).getEventProducer();
				if (producer != null) {
					synchronized (producer) {
						producer.shutdown();
					}
				}
				readerTaskIterator.remove(); // remove from map
			}
		}
		return res;
	}

    public Map<String, ReaderTask> getReaderTasks() {
        return readerTaskMap;
    }

	public Set<String> getCurrentReadTaskSet() {
		return readerTaskMap.keySet();
	}

	/**
	 * SessionManager and Listener，command is also used
	 * @param readerTaskName
	 * @return
	 */
	@Deprecated
	public EventProducer getEventProducer(String readerTaskName) {
		ReaderTask readerTask = readerTaskMap.get(readerTaskName);
		return Objects.nonNull(readerTask) ? readerTask.getEventProducer() : null;
	}

	public void registerReaderTask(ConfigService configService, String readerTaskName) {
		if (isStart()) {
			if (!readerTaskMap.containsKey(readerTaskName)) {
				ProducerBaseConfig producerBaseConfig = ProducerBaseConfigBuilder.BUILDER.build(configService, readerTaskName);
				// Attention: The sourceType field does not exist in the old version configuration
				SourceType sourceType = Objects.nonNull(producerBaseConfig.getSourceType()) ? producerBaseConfig.getSourceType() : SourceType.MySQL;
				switch (sourceType) {
					case MySQL:
						registerMySQLReaderTask(configService, readerTaskName);
						break;
					default:
						LOG.error("Unsupported source type {} of reader task {}", producerBaseConfig.getSourceType(), readerTaskName);
						throw new PtubesRunTimeException("Unknown source type of reader task");
				}
			} else {
				LOG.warn("Duplicated Reader Task register to Reader Task Manager, skip operation");
			}
		} else {
			LOG.error("reader task manager is not working, add reader task {} fail", readerTaskName);
			throw new PtubesRunTimeException("reader task manager is not working, fail to add reader task " + readerTaskName);
		}
	}

	private void registerMySQLReaderTask(ConfigService configService, String readerTaskName) {
		ReplicatorEventProducer producer = null;
		try {
			MySQLConfigRespository mySQLConfigRespository = MySQLConfigRespositoryBuilder.INSTANCE.build(configService, readerTaskName);
			ProducerConfig producerConfig = mySQLConfigRespository.getProducerConfig();
			StorageConfig storageConfig = mySQLConfigRespository.getStorageConfig();
			ReaderTaskStatMetricsCollector metricsCollector = new ReaderTaskStatMetricsCollector(readerTaskName);
			producer = new ReplicatorEventProducer(producerConfig, storageConfig, metricsCollector);
			producer.start();

			aggregatedReadTaskStatMetricsCollector.registerReaderTaskStatMetricsCollector(readerTaskName, metricsCollector);
			ReaderTask readerTask = new ReaderTask(readerTaskName, producer);
			readerTaskMap.put(readerTaskName, readerTask);

			LOG.info("readTask {} settle in manger :-D", readerTaskName);
		} catch (Throwable te) {
			if (producer != null) {
				producer.shutdown();
			}
			LOG.error("setup readTask {} error", readerTaskName, te);
			throw new PtubesRunTimeException("setup readTask " + readerTaskName + " error", te);
		}
	}

	public void deregisterReaderTask(ConfigService configService, String readerTaskName) {
		ReaderTask readerTask = readerTaskMap.get(readerTaskName);
		if (Objects.nonNull(readerTask)) {
			EventProducer producer = readerTask.getEventProducer();

			synchronized (producer) {
				// here is the sync off
				producer.shutdown();
				if (Objects.nonNull(readerTask.getConfigListenerExecutor())) {
					readerTask.getConfigListenerExecutor().shutdown();
				}
				aggregatedReadTaskStatMetricsCollector.unregisterReaderTaskStatMetricsCollector(readerTaskName);

				readerTaskMap.remove(readerTaskName);
			}
			sessionManager.deregisterSessionsAsync(readerTaskName);

			LOG.info("readTask {} depart from manager /(ㄒoㄒ)/", readerTaskName);
		} else {
			LOG.warn("deregister non-exist reader task {}", readerTaskName);
		}
	}

	private ReaderRuntimeInfo compactMySQLProducerRuntimeConf(ReplicatorEventProducer eventProducer) {
		ReaderRuntimeInfo readerRuntimeInfo = null;
		String readerTaskName = eventProducer.getReaderTaskName();
		try {
			synchronized (eventProducer) {
				if (eventProducer.isRunning()) {
					Optional<ProducerConfig> producerConfig =
						JSONUtil.jsonToSimpleBean(
							JSONUtil.toJsonString(eventProducer.getProducerConfig(), false),
							ProducerConfig.class
						);
					Optional<StorageConfig> storageConfig =
						JSONUtil.jsonToSimpleBean(
							JSONUtil.toJsonString(eventProducer.getStorageConfig(), false),
							StorageConfig.class
						);
					readerRuntimeInfo = new ReaderRuntimeInfo();
					readerRuntimeInfo.setReaderTaskName(readerTaskName);
					if (producerConfig.isPresent()) {
						readerRuntimeInfo.setProducerConfig(producerConfig.get());
					}
					if (storageConfig.isPresent()) {
						readerRuntimeInfo.setStorageConfig(storageConfig.get());
					}
					return readerRuntimeInfo;
				} else {
					LOG.warn("readerTask {} producer is shutdown, skip fetchRuntimeConf", readerTaskName);
					return null;
				}
			}
		} catch (Exception e) {
			LOG.error("readerTask {} caught error when fetchRuntimeConf", readerTaskName, e);
			return null;
		}
	}

	public ReaderRuntimeInfo fetchRuntimeConf(String readerTaskName) {
		EventProducer eventProducer = readerTaskMap.get(readerTaskName).getEventProducer();
		if (eventProducer != null) {
			switch (eventProducer.getSourceType()) {
				case MySQL:
					ReplicatorEventProducer replicatorEventProducer = (ReplicatorEventProducer) eventProducer;
					return compactMySQLProducerRuntimeConf(replicatorEventProducer);
				default:
					throw new UnsupportedOperationException("Unsupported Producer of type " + eventProducer.getSourceType());
			}
		} else {
			LOG.warn("readerTask {} is not exist when fetchRuntimeConf", readerTaskName);
			return null;
		}
	}
}
