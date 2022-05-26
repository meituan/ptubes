package com.meituan.ptubes.reader.producer.mysqlreplicator;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.config.handler.ChangeIdInfoReaderWriter;
import com.meituan.ptubes.reader.container.common.config.handler.MaxBinlogInfoReaderWriter;
import com.meituan.ptubes.reader.container.common.config.handler.MetaFileReaderWriter;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig;
import com.meituan.ptubes.reader.container.common.config.producer.RdsConfig;
import com.meituan.ptubes.reader.container.common.config.producer.RdsUserPassword;
import com.meituan.ptubes.reader.container.common.config.service.ProducerConfigService;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.common.constants.ProducerConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.thread.PtubesThreadBase;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.ChangeIdInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLMaxBinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.producer.EventProducer;
import com.meituan.ptubes.reader.producer.mysqlreplicator.component.BinlogPipeline;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.Replicator;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.AbstractBinlogParser;
import com.meituan.ptubes.reader.producer.sequencer.AutoIncrementSequencer;
import com.meituan.ptubes.reader.producer.sequencer.Sequence;
import com.meituan.ptubes.reader.producer.sequencer.Sequencer;
import com.meituan.ptubes.reader.schema.provider.ReadTaskSchemaProviderV2;
import com.meituan.ptubes.reader.schema.service.FileBasedISchemaServiceV2;
import com.meituan.ptubes.reader.schema.service.ISchemaService;
import com.meituan.ptubes.reader.storage.channel.DefaultWriteChannel;
import com.meituan.ptubes.reader.storage.channel.WriteChannel;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.ThreadContext;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public class ReplicatorEventProducer implements EventProducer<MySQLMaxBinlogInfo> {

	private final Logger log;
	private final SourceType sourceType = SourceType.MySQL;

	private final ProducerConfig producerConfig;
	private final RdsUserPassword rdsUserPassword;
	private final StorageConfig storageConfig;
	private final ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;
	private final static long BINLOG_MS_TIME_BACK = 120000;
	private MaxBinlogInfoReaderWriter maxBinlogInfoReaderWriter;
	private ChangeIdInfoReaderWriter changeIdInfoReaderWriter;
	private AbstractBinlogParser.Context oldContext;
	private volatile EventProducerThread producerThread;
	private ISchemaService schemaService;
	private ReadTaskSchemaProviderV2 readTaskSchemaProvider;
	private final ProducerConfigService producerConfigService;

	private final Sequencer sequencer;

	@Override public ProducerConfig getProducerConfig() {
		return producerConfig;
	}

	@Override
	public StorageConfig getStorageConfig() {
		return storageConfig;
	}

	@Override public void pushForwardCheckpoint(BinlogInfo binlogInfo) {
		// do nothing
	}

	public ReplicatorEventProducer(ProducerConfig producerConfig, StorageConfig storageConfig, ReaderTaskStatMetricsCollector readerTaskCollector) throws PtubesException {
		this.log = LoggerFactory.getLogger(getClass().getName() + "_" + producerConfig.getReaderTaskName());
		this.producerConfig = producerConfig;
		this.storageConfig = storageConfig;
		this.readerTaskStatMetricsCollector = readerTaskCollector;
		changeIdInfoReaderWriter = new ChangeIdInfoReaderWriter(producerConfig.getReaderTaskName());
		maxBinlogInfoReaderWriter = new MaxBinlogInfoReaderWriter(producerConfig.getReaderTaskName(),
				producerConfig.getProducerBaseConfig(), new MySQLMaxBinlogInfo(changeIdInfoReaderWriter.getCurrentChangeId(), producerConfig.getRdsConfig().getServerId()));

		this.producerConfigService = new ProducerConfigService(producerConfig);
		this.schemaService = new FileBasedISchemaServiceV2(
				ContainerConstants.BASE_DIR + File.separator + producerConfig.getReaderTaskName() + File.separator
						+ ProducerConstants.SCHEMA_BASE_DIR);
		this.rdsUserPassword = getPassword(
			producerConfig.getReaderTaskName(),
			producerConfig.getRdsConfig()
		);
		this.readTaskSchemaProvider = new ReadTaskSchemaProviderV2(producerConfig.getReaderTaskName(),
																 producerConfig.getRdsConfig(),
																 this.rdsUserPassword,
																 this.schemaService,
																 this.producerConfigService
		);

		this.sequencer = new AutoIncrementSequencer();
	}

	private RdsUserPassword getPassword(
		String readerTaskName,
		RdsConfig rdsConfig
	) throws PtubesException {
		if (StringUtils.isEmpty(rdsConfig.getUserName()) ||
			StringUtils.isEmpty(rdsConfig.getPassword())) {
			throw new PtubesException(
				"ReaderTask: " + readerTaskName + " has no available userName and password");
		} else {
			log.info("ReaderTask: {} use password from rds config", readerTaskName);
			return new RdsUserPassword(
				rdsConfig.getUserName(),
				rdsConfig.getPassword()
			);
		}
	}

	@Override
	public String getReaderTaskName() {
		return producerConfig.getReaderTaskName();
	}

	@Override
	public SourceType getSourceType() {
		return sourceType;
	}

	@Override public ReaderTaskStatMetricsCollector getMetricsCollector() {
		return readerTaskStatMetricsCollector;
	}

	private void start(
		String readerTaskName,
		MySQLMaxBinlogInfo mySQLMaxBinlogInfo
	) {
		this.readTaskSchemaProvider.start();
		this.sequencer.resetSequence(Sequence.of(mySQLMaxBinlogInfo.getIncrementalLabel()));

		producerThread = new EventProducerThread(readerTaskName, mySQLMaxBinlogInfo, null);
		producerThread.start(false);
	}

	@Override
	public synchronized void start() {
		start(
			producerConfig.getReaderTaskName(),
			maxBinlogInfoReaderWriter.getLastSaveMaxBinlogInfo()
		);
	}

	@Override
	public synchronized void start(MySQLMaxBinlogInfo maxBinlogInfo) {
		throw new UnsupportedOperationException("unsupported MySQL replica to start with maxBinlogInfo");
	}

	@Override
	public synchronized void start(long timeInMS) {
		MySQLMaxBinlogInfo currMySQLMaxBinlogInfo = maxBinlogInfoReaderWriter.getLastSaveMaxBinlogInfo();

		MetaFileReaderWriter.backupProducerDir(producerConfig.getReaderTaskName());

		MySQLMaxBinlogInfo defaultMaxBinlogInfo = new MySQLMaxBinlogInfo(
			changeIdInfoReaderWriter.getCurrentChangeId(),
			producerConfig.getRdsConfig().getServerId()
		);
		defaultMaxBinlogInfo.setIncrementalLabel(currMySQLMaxBinlogInfo.getIncrementalLabel());

		changeIdInfoReaderWriter = new ChangeIdInfoReaderWriter(producerConfig.getReaderTaskName());
		maxBinlogInfoReaderWriter = new MaxBinlogInfoReaderWriter(
			producerConfig.getReaderTaskName(),
			producerConfig.getProducerBaseConfig(),
			defaultMaxBinlogInfo
		);

		MySQLMaxBinlogInfo mySQLMaxBinlogInfo = new MySQLMaxBinlogInfo(
			maxBinlogInfoReaderWriter.getLastSaveMaxBinlogInfo().getChangeId(),
			maxBinlogInfoReaderWriter.getLastSaveMaxBinlogInfo().getServerId()
		);
		mySQLMaxBinlogInfo.setBinlogTime(timeInMS);
		mySQLMaxBinlogInfo.setIncrementalLabel(currMySQLMaxBinlogInfo.getIncrementalLabel());

		maxBinlogInfoReaderWriter.saveMaxBinlogInfoDirectly(mySQLMaxBinlogInfo);

		oldContext = null;

		start(
			producerConfig.getReaderTaskName(),
			mySQLMaxBinlogInfo
		);
	}

	@Override
	public synchronized void start(
		int binlogId,
		long binlogOffset
	) {
		MySQLMaxBinlogInfo currMySQLMaxBinlogInfo = maxBinlogInfoReaderWriter.getLastSaveMaxBinlogInfo();

		MetaFileReaderWriter.backupProducerDir(producerConfig.getReaderTaskName());

		MySQLMaxBinlogInfo defaultBinlogInfo = new MySQLMaxBinlogInfo(
			changeIdInfoReaderWriter.getCurrentChangeId(),
			producerConfig.getRdsConfig().getServerId()
		);
		defaultBinlogInfo.setIncrementalLabel(currMySQLMaxBinlogInfo.getIncrementalLabel());

		changeIdInfoReaderWriter = new ChangeIdInfoReaderWriter(producerConfig.getReaderTaskName());
		maxBinlogInfoReaderWriter = new MaxBinlogInfoReaderWriter(
			producerConfig.getReaderTaskName(),
			producerConfig.getProducerBaseConfig(),
			defaultBinlogInfo
		);

		MySQLMaxBinlogInfo mySQLMaxBinlogInfo = new MySQLMaxBinlogInfo(
			maxBinlogInfoReaderWriter.getLastSaveMaxBinlogInfo().getChangeId(),
			maxBinlogInfoReaderWriter.getLastSaveMaxBinlogInfo().getServerId()
		);
		mySQLMaxBinlogInfo.setBinlogId(binlogId);
		mySQLMaxBinlogInfo.setBinlogOffset(binlogOffset);
		mySQLMaxBinlogInfo.setIncrementalLabel(currMySQLMaxBinlogInfo.getIncrementalLabel());

		maxBinlogInfoReaderWriter.saveMaxBinlogInfoDirectly(mySQLMaxBinlogInfo);

		oldContext = null;

		start(
			producerConfig.getReaderTaskName(),
			mySQLMaxBinlogInfo
		);
	}

	@Override
	public synchronized boolean isRunning() {
		return producerThread != null && !producerThread.isShutdownRequested() && !producerThread.isPauseRequested();
	}

	@Override
	public synchronized boolean isPaused() {
		return producerThread != null && producerThread.isPauseRequested();
	}

	@Override
	public synchronized void unpause() {
		if (producerThread == null) {
			return;
		}
		try {
			producerThread.unpause();
		} catch (InterruptedException e) {
			log.info("Interrupted while unpausing EventProducer", e);
		}
	}

	@Override
	public synchronized void pause() {
		if (producerThread == null) {
			return;
		}
		try {
			producerThread.pause();
		} catch (InterruptedException e) {
			log.info("Interrupted while pausing EventProducer", e);
		}
	}

	@Override
	public synchronized void shutdown() {
		// Interrupt the thread so it can shut down, in case it is waiting on the sleep timer
		log.info(">>>>> readerTask: {} begin to shutdown and release event producer sources", this.producerConfig.getReaderTaskName());
		if (producerThread != null) {
			producerThread.shutdownWithWriteChannel();
		}
		if (readTaskSchemaProvider != null) {
			readTaskSchemaProvider.stop();
		}
		log.info(">>>>> readerTask: {} shutdown and release event producer sources completely", this.producerConfig.getReaderTaskName());
	}

	@Override
	public void waitForShutdown() throws InterruptedException, IllegalStateException {
		while (null != producerThread && producerThread.isAlive()) {
			producerThread.join();
		}
	}

	@Override
	public void waitForShutdown(long timeout) throws InterruptedException, IllegalStateException {
		if (null != producerThread && producerThread.isAlive()) {
			producerThread.join(timeout);
		}

		if (null != producerThread && producerThread.isAlive()) {
			throw new IllegalStateException();
		}

	}

	@Override
	public Pair<BinlogInfo, BinlogInfo> getBinlogInfoRange() {
		return this.producerThread.getWriteChannel().getBinlogInfoRange();
	}

	@Override
	public StorageConstant.StorageRangeCheckResult isInStorageRange(BinlogInfo binlogInfo) {
		try {
			assert binlogInfo instanceof MySQLBinlogInfo;
			ChangeIdInfoReaderWriter.fillChangeId(storageConfig.getReaderTaskName(), (MySQLBinlogInfo) binlogInfo);
		} catch (LessThanStorageRangeException e) {
			return StorageConstant.StorageRangeCheckResult.LESS_THAN_MIN;
		}
		return this.producerThread.getWriteChannel().isInStorage(binlogInfo);
	}

	/**
	 * switch storage mode
	 *
	 * @param storageMode enum as [MEM, MIX]
	 * @throws InterruptedException
	 */
	@Override
	public void switchStorageMode(StorageConstant.StorageMode storageMode) throws InterruptedException {
		this.storageConfig.setStorageMode(storageMode);
		this.producerThread.getWriteChannel().switchStorageMode(storageMode);
	}

	public class EventProducerThread extends PtubesThreadBase {
		private String name;
		private final long reconnectIntervalMs = 2000;
		private final long workIntervalMs = 100;
		private final int maxQueueSize = 300;
		private Replicator replicator;

		private BinlogPipeline binlogPipeline;
		private WriteChannel writeChannel = null;
		private MySQLMaxBinlogInfo mySQLMaxBinlogInfo;

		public BinlogPipeline getBinlogPipeline() {
			return binlogPipeline;
		}

		public WriteChannel getWriteChannel() {
			return writeChannel;
		}

		public EventProducerThread(String sourceName, MySQLMaxBinlogInfo mySQLMaxBinlogInfo, WriteChannel writeChannel) {
			super("EventProducerThread_" + sourceName);
			this.name = sourceName; // taskName
			this.mySQLMaxBinlogInfo = mySQLMaxBinlogInfo;
			this.replicator = new Replicator(
				producerConfig.getReaderTaskName(),
				producerConfig.getRdsConfig(),
				rdsUserPassword,
				RandomUtils.nextInt(Integer.MAX_VALUE)
			);
			this.writeChannel = writeChannel;
		}

		private MySQLBinlogInfo getStartBinlogInfo(MySQLMaxBinlogInfo fromBinlogInfo) throws IOException {
			boolean serverIdChanged = fromBinlogInfo.getServerId() != producerConfig.getRdsConfig().getServerId();
			short changeId = (short) fromBinlogInfo.getChangeId();
			if (serverIdChanged) {
				changeId = changeIdInfoReaderWriter.getNextChangeId();
			}

			MySQLBinlogInfo startBinlogInfo;
			ChangeIdInfo changeIdInfo;
			if (storageConfig.getStorageMode().equals(StorageConstant.StorageMode.FILE)
				|| storageConfig.getStorageMode().equals(StorageConstant.StorageMode.MIX)
			) {
				FileSystem.recover(storageConfig, sourceType);
				MySQLBinlogInfo storeMaxBinlogInfo = (MySQLBinlogInfo) FileSystem.getMaxBinlogInfo(storageConfig, sourceType);
				if (!serverIdChanged && storeMaxBinlogInfo.getTimestamp() > fromBinlogInfo.getBinlogTime()) {
					startBinlogInfo = new MySQLBinlogInfo(
						changeId,
						fromBinlogInfo.getServerId(),
						fromBinlogInfo.getBinlogId(),
						fromBinlogInfo.getBinlogOffset(),
						fromBinlogInfo.getGtid().getUuid(),
						fromBinlogInfo.getGtid().getTransactionId(),
						fromBinlogInfo.getEventIndex(),
						fromBinlogInfo.getBinlogTime()
					);
				} else {
					long time = storeMaxBinlogInfo.getTimestamp() - BINLOG_MS_TIME_BACK;
					startBinlogInfo = new MySQLBinlogInfo(
						changeId,
						producerConfig.getRdsConfig().getServerId(),
						-1,
						-1,
						new byte[16],
						-1,
						0,
						time
					);
				}
				changeIdInfo = new ChangeIdInfo(startBinlogInfo.getChangeId(), startBinlogInfo.getServerId(),
						"", startBinlogInfo.getBinlogId(), startBinlogInfo.getBinlogOffset(),
						startBinlogInfo.getTimestamp());
			} else {
				if (serverIdChanged) {
					// changing binlog source
					long time = fromBinlogInfo.getBinlogTime() - BINLOG_MS_TIME_BACK;
					startBinlogInfo = new MySQLBinlogInfo(changeId, producerConfig.getRdsConfig().getServerId(), -1, -1,
													 new byte[16], -1, 0, time);
					changeIdInfo = new ChangeIdInfo(startBinlogInfo.getChangeId(), startBinlogInfo.getServerId(),
							"", startBinlogInfo.getBinlogId(), startBinlogInfo.getBinlogOffset(),
							startBinlogInfo.getTimestamp());
				} else {
					// restart
					startBinlogInfo = new MySQLBinlogInfo(
						changeId,
						fromBinlogInfo.getServerId(),
						fromBinlogInfo.getBinlogId(),
						fromBinlogInfo.getBinlogOffset(),
						fromBinlogInfo.getGtid().getUuid(),
						fromBinlogInfo.getGtid().getTransactionId(),
						fromBinlogInfo.getEventIndex(),
						fromBinlogInfo.getBinlogTime()
					);
					changeIdInfo = new ChangeIdInfo(startBinlogInfo.getChangeId(), startBinlogInfo.getServerId(),
							fromBinlogInfo.getGtidSet(), startBinlogInfo.getBinlogId(), startBinlogInfo.getBinlogOffset(),
							startBinlogInfo.getTimestamp());
				}
			}

			changeIdInfoReaderWriter.addChangeIdInfo(changeIdInfo);

			startBinlogInfo.setIncrementalLabel(fromBinlogInfo.getIncrementalLabel());
			return startBinlogInfo;
		}

		void initReplicatorListener(MySQLBinlogInfo startBinlogInfo) {
			if (writeChannel == null) {
				writeChannel = new DefaultWriteChannel(storageConfig, startBinlogInfo, readerTaskStatMetricsCollector, sourceType);
				try {
					writeChannel.start();
				} catch (Exception e) {
					throw new PtubesRunTimeException(e);
				}
			}

			sequencer.resetSequence(Sequence.of(startBinlogInfo.getIncrementalLabel()));
			binlogPipeline = new BinlogPipeline(ReplicatorEventProducer.this, producerConfigService, writeChannel,
				maxBinlogInfoReaderWriter, readTaskSchemaProvider, 2048, maxBinlogInfoReaderWriter.getLastSaveMaxBinlogInfo(),
				startBinlogInfo.getChangeId(), readerTaskStatMetricsCollector, sequencer);
		}

		void startProducerFromStorage(MySQLMaxBinlogInfo fromBinlogInfo) throws Exception {
			MySQLBinlogInfo startBinlogInfo = getStartBinlogInfo(fromBinlogInfo);

			startProducer(startBinlogInfo, "");
		}

		void startProducerFromLocalMaxBinlogInfo(MySQLMaxBinlogInfo fromBinlogInfo) throws Exception {
			MySQLBinlogInfo startBinlogInfo;
			if (fromBinlogInfo.getServerId() != producerConfig.getRdsConfig().getServerId()) {
				// switching binlog source
				short changeId = changeIdInfoReaderWriter.getNextChangeId();
				long time = StringUtils.isBlank(fromBinlogInfo.getGtidSet()) ? fromBinlogInfo.getBinlogTime() - BINLOG_MS_TIME_BACK : fromBinlogInfo.getBinlogTime();
				ChangeIdInfo changeIdInfo = new ChangeIdInfo(
					changeId,
					producerConfig.getRdsConfig().getServerId(),
					fromBinlogInfo.getGtidSet(),
					-1,
					-1,
					time
				);
				changeIdInfoReaderWriter.addChangeIdInfo(changeIdInfo);

				MySQLMaxBinlogInfo mySQLMaxBinlogInfo = new MySQLMaxBinlogInfo(
					changeId,
					producerConfig.getRdsConfig().getServerId()
				);
				mySQLMaxBinlogInfo.setBinlogTime(time);
				mySQLMaxBinlogInfo.setIncrementalLabel(fromBinlogInfo.getIncrementalLabel());
				maxBinlogInfoReaderWriter.saveMaxBinlogInfoDirectly(mySQLMaxBinlogInfo);

				startBinlogInfo = new MySQLBinlogInfo(
					changeId,
					changeIdInfo.getServerId(),
					-1,
					-1,
					new byte[16],
					-1,
					-1,
					time
				);
			} else {
				startBinlogInfo = new MySQLBinlogInfo(
					(short) fromBinlogInfo.getChangeId(),
					fromBinlogInfo.getServerId(),
					fromBinlogInfo.getBinlogId(),
					fromBinlogInfo.getBinlogOffset(),
					fromBinlogInfo.getGtid().getUuid(),
					fromBinlogInfo.getGtid().getTransactionId(),
					fromBinlogInfo.getEventIndex(),
					fromBinlogInfo.getBinlogTime()
				);
				ChangeIdInfo changeIdInfo = new ChangeIdInfo(
					startBinlogInfo.getChangeId(),
					startBinlogInfo.getServerId(),
					fromBinlogInfo.getGtidSet(),
					startBinlogInfo.getBinlogId(),
					startBinlogInfo.getBinlogOffset(),
					startBinlogInfo.getTimestamp()
				);
				changeIdInfoReaderWriter.addChangeIdInfo(changeIdInfo);
			}

			startBinlogInfo.setIncrementalLabel(fromBinlogInfo.getIncrementalLabel());
			startProducer(startBinlogInfo, fromBinlogInfo.getGtidSet());
		}

		void startProducer(MySQLBinlogInfo startBinlogInfo, String gtidSetStr) throws Exception {
			initReplicatorListener(startBinlogInfo);

			oldContext = replicator.getBinlogParser() != null ? replicator.getBinlogParser().getContext() : null;
			String binlogFileName = String.format("%s.%06d",
					producerConfig.getProducerBaseConfig().getBinlogFilePrefix(), startBinlogInfo.getBinlogId());

			replicator.reInit(binlogFileName, startBinlogInfo.getBinlogOffset(), gtidSetStr, binlogPipeline/*replicatorListener*/);

			log.info("Replicator starting from binlogId: {}, binlogOffset: {}, time: {}, gtidSet: {}, sequence={}, taskName: {}",
					startBinlogInfo.getBinlogId(), startBinlogInfo.getBinlogOffset(), startBinlogInfo.getTimestamp(),
					gtidSetStr, startBinlogInfo.getIncrementalLabel(), producerConfig.getReaderTaskName());

			// Attention: The pipeline startup is split into two parts, first prepare the resources, so that the replicator can be started
			binlogPipeline.prepare();
			if (startBinlogInfo.getBinlogId() < 0) {
				replicator.start(startBinlogInfo.getTimestamp());
			} else if (StringUtils.isBlank(gtidSetStr)) {
				replicator.start(binlogFileName, startBinlogInfo.getBinlogOffset());
			} else {
				replicator.start(oldContext);
			}
			// Attention: The pipeline can be started only if the replicator is successfully started (the database is available),
			// otherwise the Disruptor will not be closed and the thread will leak
			// Start the pipeline first, if the replicator is unavailable,
			// then the method of catching the exception and then stopping the Disruptor will be risky,
			// The shutdown of the Disruptor will only close the worker that has been successfully started.
			// If the worker only starts part of the stop when it stops, it will also leak the thread.
			binlogPipeline.start();
		}

		public void start(boolean startWithRecover) {
			if (startWithRecover) {
				try {
					startProducerFromStorage(mySQLMaxBinlogInfo);
				} catch (Exception e) {
					log.error(
						"failed to start replicator, taskName: {}",
						producerConfig.getReaderTaskName(),
						e
					);
					throw new PtubesRunTimeException(
						"failed to start open replicator, taskName: " + producerConfig.getReaderTaskName() + ", msg: " +
							e.getMessage(),
						e
					);
				}
			} else {
				try {
					startProducerFromLocalMaxBinlogInfo(mySQLMaxBinlogInfo);
				} catch (Exception e) {
					log.error(
						"failed to start replicator, taskName: {}",
						producerConfig.getReaderTaskName(),
						e
					);
					throw new PtubesRunTimeException(
						"failed to start open replicator, taskName: " + producerConfig.getReaderTaskName() + ", msg: " +
							e.getMessage(),
						e
					);
				}
			}
			super.start();
		}

		@Override
		public void run() {
			ThreadContext.put("TASKNAME", name);
			long lastConnectMs = System.currentTimeMillis();
			while (!isShutdownRequested()) {
				if (isPauseRequested()) {
					log.info("Pause requested for Replicator. Pausing !!");
					signalPause();
					log.info("Pausing. Waiting for resume command");
					try {
						if (binlogPipeline.isAlive()) {
							binlogPipeline.pause();
						}
						awaitUnPauseRequest();
					} catch (InterruptedException e) {
						log.info("Interrupted !!");
					}
					log.info("Resuming Replicator !!");
					if (binlogPipeline.isAlive()) {
						try {
							binlogPipeline.unpause();
						} catch (InterruptedException e) {
							log.info("Interrupted !!");
						}
					}
					signalResumed();
					log.info("Replicator resumed !!");
				}

				long lastEventTime = -1;
				try {
					lastEventTime = replicator.millisSinceLastEvent() == null ? -1 : replicator.millisSinceLastEvent();
				} catch (Exception e) {
					log.error(e.getMessage(), e);
				}
				try {
					lastEventTime = lastEventTime > binlogPipeline.millisSinceLastEvent() ? lastEventTime : /*replicatorListener*/binlogPipeline.millisSinceLastEvent();
				} catch (Exception e) {
					log.error(e.getMessage(), e);
				}

				if (producerConfig.getProducerBaseConfig().getNoEventsConnectionResetTimeSec() > 0 && lastEventTime
						> producerConfig.getProducerBaseConfig().getNoEventsConnectionResetTimeSec() * 1000) {
					try {
						log.info("No events more than " + producerConfig.getProducerBaseConfig()
								.getNoEventsConnectionResetTimeSec() + " secs, replicator will be reconnect!");
						log.info("replicator : " + replicator.millisSinceLastEvent() + "" + " | replicatorListener : "
								+ binlogPipeline.millisSinceLastEvent());
						replicator.stopQuietly(10, TimeUnit.SECONDS);
					} catch (Exception e) {
						log.error("Close replicator error!", e);
					}
				}

				if (!replicator.isRunning() && System.currentTimeMillis() - lastConnectMs > reconnectIntervalMs) {
					log.info("Replicator not running " + reconnectIntervalMs + "ms");
					lastConnectMs = System.currentTimeMillis();
					try {
						//should stop binlog pipeline first to get the final maxScn used for startProducer open replicator.
						binlogPipeline.stop();

						MySQLMaxBinlogInfo newMySQLMaxBinlogInfo = maxBinlogInfoReaderWriter.getLastSaveMaxBinlogInfo();
						startProducerFromLocalMaxBinlogInfo(newMySQLMaxBinlogInfo);
						log.info("start Open Replicator successfully");
					} catch (Exception e) {
						log.error("failed to start Open Replicator", e);
						if (replicator.isRunning()) {
							try {
								replicator.stop(10, TimeUnit.SECONDS);
							} catch (Exception e2) {
								log.error("failed to stop Open Replicator", e2);
							}
						}
					}
				}

				try {
					Thread.sleep(workIntervalMs);
				} catch (InterruptedException e) {
					log.info("Interrupted !!");
				}
			}

			// do shutdown
			if (replicator.isRunning()) {
				try {
					replicator.stop(10, TimeUnit.SECONDS);
				} catch (Exception e) {
					log.error("failed to stop Open Replicator", e);
				}
			}
			binlogPipeline.stop();
			log.info("Event Producer Thread done");
			doShutdownNotify();
		}

		public synchronized void shutdownWithWriteChannel() {
			log.info(">>>>>> readerTask: {} producer begin to shutdown", producerConfig.getReaderTaskName());
			if (this.isAlive()) {
				super.shutdown();
			}
			if (this.writeChannel != null) {
				this.writeChannel.stop();
			}
			log.info(">>>>>> readerTask: {} producer shutdown completely", producerConfig.getReaderTaskName());
		}

	}
}
