package com.meituan.ptubes.reader.container;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.AbstractLifeCycle;
import com.meituan.ptubes.reader.container.common.config.ConfigService;
import com.meituan.ptubes.reader.container.common.config.FileConfigService;
import com.meituan.ptubes.reader.container.common.config.netty.ServerConfig;
import com.meituan.ptubes.reader.container.config.ReaderTaskSetConfigBuilder;
import com.meituan.ptubes.reader.container.config.ServerConfigBuilder;
import com.meituan.ptubes.reader.container.manager.CollectorManager;
import com.meituan.ptubes.reader.container.manager.ReaderTaskManager;
import com.meituan.ptubes.reader.container.manager.SessionManager;
import com.meituan.ptubes.reader.container.network.MonitorServer;
import com.meituan.ptubes.reader.container.network.ReaderServer;
import com.meituan.ptubes.reader.container.network.connections.DefaultClientSessionFactory;
import com.meituan.ptubes.reader.container.network.connections.IClientSessionFactory;
import com.meituan.ptubes.reader.monitor.collector.AggregatedClientSessionStatMetricsCollector;
import com.meituan.ptubes.reader.monitor.collector.AggregatedReadTaskStatMetricsCollector;
import java.util.Collections;
import java.util.Set;

public class Container extends AbstractLifeCycle {

	private static final Logger LOG = LoggerFactory.getLogger(Container.class);

	// managers
	private ReaderTaskManager readerTaskManager;
	private SessionManager sessionManager;
	private CollectorManager collectorManager;

	// active servers
	private ReaderServer readerServer;
	private MonitorServer monitorServer;

	// configuration service
	private ConfigService configService;

	public Container() {
		this(FileConfigService.getInstance());
	}

	public Container(ConfigService configService) {
		this.configService = configService;
	}

	// Only those who only change a certain configuration value can push directly with lion
	@Override
	public boolean start() {
		boolean res = super.start();
		if (res) {
			/** God created the world in six days
			 * 1. load global configs (hard coding) and parse
			 * 2. setup global collector
			 * 3. setup session/readTask manager
			 * 4. setup servers
			 * 5. setup readTask schedule loader
			 * 6. setup container shutdown hook
			 */

			LOG.info("container will start in a few seconds...");
			this.configService.initialize();

			// init task managers
			this.collectorManager = new CollectorManager();
			IClientSessionFactory sessionFactory = DefaultClientSessionFactory.getInstance();
			AggregatedClientSessionStatMetricsCollector clientMetricsCollector = collectorManager.getAggregatedClientSessionStatMetricsCollector();
			AggregatedReadTaskStatMetricsCollector readerMetricsCollector = collectorManager.getAggregatedReadTaskStatMetricsCollector();
			this.sessionManager = new SessionManager(sessionFactory, clientMetricsCollector, readerMetricsCollector);
			this.readerTaskManager = new ReaderTaskManager(sessionManager, readerMetricsCollector);
			this.sessionManager.start();
			this.readerTaskManager.start();
			this.collectorManager.start();

			// init netty servers
			ServerConfig serverConfig = ServerConfigBuilder.INSTANCE.build(configService);
			this.readerServer = new ReaderServer(this, serverConfig);
			this.readerServer.start();
			this.monitorServer = new MonitorServer(this, serverConfig);
			this.monitorServer.start();

			// register reader tasks
			Set<String> taskList = Collections.unmodifiableSet(ReaderTaskSetConfigBuilder.BUILDER.build(configService));
			taskList.stream().forEach(readerTaskName -> {
				LOG.info("welcome readTask {}", readerTaskName);
				try {
					readerTaskManager.registerReaderTask(configService, readerTaskName);
				} catch (Throwable te) {
					LOG.error("fail to register reader task " + readerTaskName, te);
				}
			});

			Runtime.getRuntime().addShutdownHook(new Thread(() -> { Container.this.stop(); }));
			LOG.info("container start!");
		} else {
			LOG.info("container already started! do nothing!");
		}
		return res;
	}

	/** never used */
	@Override
	public boolean stop() {
		boolean res = super.stop();
		if (res) {

			/**
			 * 1. readTask loader
			 * 2. server stop (all session will be closed)
			 * 3. stop readTaskManager
			 * 4. stop session manager
			 * 5. stop collector?
			 * 6. process exit?
			 */
			LOG.info("container will be stopped...");

			this.configService.destroy();
			this.readerServer.stop();
			this.monitorServer.stop();
			this.collectorManager.stop();
			this.readerTaskManager.stop();
			this.sessionManager.stop();
			LOG.info("container stop!");
		} else {
			LOG.info("container already stopped! do nothing!");
		}
		return res;
	}

	public ReaderTaskManager getReaderTaskManager() {
		return readerTaskManager;
	}

	public SessionManager getSessionManager() {
		return sessionManager;
	}

	public CollectorManager getCollectorManager() {
		return collectorManager;
	}
}
