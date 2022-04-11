package com.meituan.ptubes.reader.producer.mysqlreplicator.network;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.reader.container.common.config.producer.RdsConfig;
import com.meituan.ptubes.reader.container.common.config.producer.RdsUserPassword;
import com.meituan.ptubes.reader.container.common.vo.GtidSet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventListener;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserListener;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.filter.BinlogEventFilter;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.filter.BinlogOffsetFilter;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.filter.TimeEventFilter;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.error.ORException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.error.TransportException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.BinlogEventV4HeaderImpl;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.DeleteRowsEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.DeleteRowsEventV2Parser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.FormatDescriptionEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.GtidEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.IncidentEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.IntvarEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.QueryEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.RandEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.RotateEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.RowsQueryEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.StopEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.TableMapEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.UpdateRowsEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.UpdateRowsEventV2Parser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.UserVarEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.WriteRowsEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.WriteRowsEventV2Parser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.XidEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.EOFPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.ErrorPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.OKPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.command.ComBinlogDumpGtidPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.command.ComBinlogDumpPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.component.ReplicationBasedBinlogParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Packet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Transport;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.TransportInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.AbstractBinlogParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.AuthenticatorImpl;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.EventInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.Query;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.SocketFactoryImpl;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.TransportImpl;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Replicator {
	protected String readerTaskName;
	protected RdsConfig rdsConfig;
	protected RdsUserPassword rdsUserPassword;
	protected long serverId = 6789;
	protected String binlogFileName;
	protected long binlogPosition = 4;
	protected String encoding = "utf-8";
	protected int level1BufferSize = 1024 * 1024;
	protected int level2BufferSize = 8 * 1024 * 1024;
	protected int socketReceiveBufferSize = 512 * 1024;
	protected Float heartbeatPeriod = 0.5F;
	protected String uuid = UUID.randomUUID().toString();
	protected boolean stopOnEOF = false;

	protected GtidSet gtidSet;
	protected Transport transport;
	protected ReplicationBasedBinlogParser binlogParser;
	protected BinlogEventListener binlogEventListener;
	protected final AtomicBoolean running = new AtomicBoolean(false);
	protected BinlogParserListener.Adapter binlogNotifyListener;
	protected BinlogEventFilter binlogEventFilter = null;

	private static final Logger LOG = LoggerFactory.getLogger(Replicator.class);

	public Replicator(String readerTaskName, RdsConfig rdsConfig, RdsUserPassword rdsUserPassword, int serverId) {
		this.readerTaskName = readerTaskName;
		this.rdsConfig = rdsConfig;
		this.rdsUserPassword = rdsUserPassword;
		this.serverId = serverId;
	}

	public void reInit(String binlogFileName, long binlogPosition, String gtidSet, BinlogEventListener listener) {
		this.binlogFileName = binlogFileName;
		this.binlogPosition = binlogPosition;
		this.gtidSet = new GtidSet(gtidSet);
		this.binlogEventListener = listener;
		this.binlogNotifyListener = new BinlogNotifyListener();
		this.transport = null;
		this.binlogParser = null;
	}

	public boolean isRunning() {
		return this.running.get();
	}

	public void start(long timeInMS) throws Exception {
		if (this.transport == null) {
			this.transport = getDefaultTransport();
		}

		this.transport.connect(this.rdsConfig.getIp(), this.rdsConfig.getPort());

		final Query query = new Query(this.transport);

		List<String> binlogFiles = query.getBinaryLogs();
		if (binlogFiles == null || binlogFiles.isEmpty()) {
			throw new PtubesRunTimeException(
				"Could not find any binlog file names, host: " + this.rdsConfig.getIp() + ", port: " +
					this.rdsConfig.getPort());
		}

		boolean isGtidMode = query.isGtidMode();

		this.transport.disconnect();

		String fromBinlogFileName = binlogFiles.get(0);
		for (int i = binlogFiles.size() - 1; i >= 0; i--) {
			long startTime = parseStartTime(binlogFiles.get(i));
			if (startTime < timeInMS) {
				fromBinlogFileName = binlogFiles.get(i);
				LOG.info(
					"ReaderTaskName: {} find binlogFile: {}, by time: {}",
					readerTaskName,
					fromBinlogFileName,
					timeInMS
				);
				break;
			}
		}
		this.binlogFileName = fromBinlogFileName;
		this.binlogPosition = 4;
		LOG.info(
			"ReaderTaskName: {} start dump binlog, time: {}, binlogFileName: {}",
			readerTaskName,
			timeInMS,
			this.binlogFileName
		);

		if (isGtidMode) {
			this.gtidSet = getPreviousGtids();
		}
		this.binlogEventFilter = new TimeEventFilter(timeInMS);
		this.start(null, false);
	}

	public void start(String binlogId, long binlogOffset) throws Exception {
		if (this.transport == null) {
			this.transport = getDefaultTransport();
		}

		this.transport.connect(this.rdsConfig.getIp(), this.rdsConfig.getPort());

		final Query query = new Query(this.transport);
		boolean isGtidMode = query.isGtidMode();

		this.transport.disconnect();

		this.binlogFileName = binlogId;
		this.binlogPosition = 4;
		LOG.info(
			"ReaderTaskName: {} start dump binlog, binlogFileName: {}, binlogOffset: {}",
			this.readerTaskName,
			this.binlogFileName,
			this.binlogPosition);

		if (isGtidMode) {
			this.gtidSet = getPreviousGtids();
			this.binlogEventFilter = new BinlogOffsetFilter(binlogOffset);
			this.start(null, false);
		} else {
			this.binlogPosition = binlogOffset;
			this.start(null, false);
		}
	}

	private GtidSet getPreviousGtids() throws Exception {
		this.transport.connect(this.rdsConfig.getIp(), this.rdsConfig.getPort());
		final Query query = new Query(this.transport);
		if (!query.isGtidMode()) {
			return new GtidSet("");
		} else {
			return query.getPreviousGtids(this.binlogFileName);
		}
	}

	private void readPacketMarker(TransportInputStream ts) throws IOException {
		final int packetMarker = ts.readInt(1);
		if (packetMarker != OKPacket.PACKET_MARKER) { // 0x00
			if ((byte) packetMarker == ErrorPacket.PACKET_MARKER) {
				final ErrorPacket packet = ErrorPacket.valueOf(ts.currentPacketLength(), ts.currentPacketSequence(),
						packetMarker, ts);
				throw new PtubesRunTimeException(packet.toString());
			} else if ((byte) packetMarker == EOFPacket.PACKET_MARKER) {
				throw new EOFException();
			} else {
				throw new PtubesRunTimeException("assertion failed, invalid packet marker: " + packetMarker);
			}
		}
	}

	public long parseStartTime(String binlogFileName) throws Exception {
		this.transport.connect(this.rdsConfig.getIp(), this.rdsConfig.getPort());

		try {
			this.binlogFileName = binlogFileName;
			this.binlogPosition = 4L;
			setupChecksumState();
			setupHeartbeatPeriod();
			setupSlaveUUID();
			dumpBinlog(false);
			final TransportInputStream is = this.transport.getInputStream();
			final EventInputStream es = new EventInputStream(is);
			readPacketMarker(is);
			BinlogEventV4HeaderImpl header = es.getNextBinlogHeader();

			return header.getTimestamp();
		} finally {
			this.transport.disconnect();
		}
	}
	public void start(AbstractBinlogParser.Context context) throws Exception {
		start(context, false);
	}

	public void start(AbstractBinlogParser.Context context, boolean useGtidIfGtidMode) throws Exception {
		if (!this.running.compareAndSet(false, true)) {
			return;
		}

		if (this.transport == null) {
			this.transport = getDefaultTransport();
		}

		this.transport.connect(this.rdsConfig.getIp(), this.rdsConfig.getPort());

		if (this.binlogParser == null) {
			this.binlogParser = getDefaultBinlogParser();
			this.binlogParser.setContext(context);
		}

		if (this.binlogFileName == null && gtidSet == null) {
			throw new ORException("binlogFileName or gtidSet is null");
		}

		setupChecksumState();
		setupHeartbeatPeriod();
		setupSlaveUUID();
		dumpBinlog(useGtidIfGtidMode);

		this.binlogParser.setBinlogFileName(this.binlogFileName);
		this.binlogParser.setPrevGtidSet(this.gtidSet);
		this.binlogParser.setEventListener(this.binlogEventListener);
		this.binlogParser.addParserListener(new BinlogParserListener.Adapter() {
			@Override
			public void onStop(BinlogParser parser) {
				stopQuietly(0, TimeUnit.MILLISECONDS);
			}
		});
		if (this.binlogNotifyListener != null) {
			this.binlogParser.addParserListener(this.binlogNotifyListener);
		}
		if (this.binlogEventFilter != null) {
			this.binlogParser.setEventFilter(binlogEventFilter);
		}
		this.binlogParser.start();
	}

	public void stop(long timeout, TimeUnit unit) throws Exception {
		if (!this.running.compareAndSet(true, false)) {
			return;
		}

		// disconnect the transport first: seems kinda wrong, but the parser thread can be blocked waiting for the
		// last event, and doesn't have any timeouts.  so we deal with the EOF exception thrown elsewhere in the code.
		if (this.binlogParser != null) {
			this.binlogParser.stop(timeout, unit);
		}

		if (this.transport != null) {
			this.transport.disconnect();
		}

		this.onStop();
	}

	public void stopQuietly(long timeout, TimeUnit unit) {
		try {
			stop(timeout, unit);
		} catch (Exception e) {
			// NOP
		}
	}

	public void stop() throws Exception {
		if (!this.running.compareAndSet(true, false)) {
			return;
		}

		// disconnect the transport first: seems kinda wrong, but the parser thread can be blocked waiting for the
		// last event, and doesn't have any timeouts.  so we deal with the EOF exception thrown elsewhere in the code.
		this.binlogParser.stop();

		this.transport.disconnect();

		this.onStop();
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public long getServerId() {
		return serverId;
	}

	public void setServerId(long serverId) {
		this.serverId = serverId;
	}

	public long getBinlogPosition() {
		return binlogPosition;
	}

	public void setBinlogPosition(long binlogPosition) {
		this.binlogPosition = binlogPosition;
	}

	public String getBinlogFileName() {
		return binlogFileName;
	}

	public void setBinlogFileName(String binlogFileName) {
		this.binlogFileName = binlogFileName;
	}

	public int getLevel1BufferSize() {
		return level1BufferSize;
	}

	public void setLevel1BufferSize(int level1BufferSize) {
		this.level1BufferSize = level1BufferSize;
	}

	public int getLevel2BufferSize() {
		return level2BufferSize;
	}

	public void setLevel2BufferSize(int level2BufferSize) {
		this.level2BufferSize = level2BufferSize;
	}

	public int getSocketReceiveBufferSize() {
		return socketReceiveBufferSize;
	}

	public void setSocketReceiveBufferSize(int socketReceiveBufferSize) {
		this.socketReceiveBufferSize = socketReceiveBufferSize;
	}

	public void setBinlogNotifyListener(BinlogParserListener.Adapter binlogNotifyListener) {
		this.binlogNotifyListener = binlogNotifyListener;
	}

	public Transport getTransport() {
		return transport;
	}

	public void setTransport(Transport transport) {
		this.transport = transport;
	}

	public BinlogParser getBinlogParser() {
		return binlogParser;
	}

	public void setBinlogParser(ReplicationBasedBinlogParser parser) {
		this.binlogParser = parser;
	}

	public BinlogEventListener getBinlogEventListener() {
		return binlogEventListener;
	}

	public void setBinlogEventListener(BinlogEventListener listener) {
		this.binlogEventListener = listener;
	}

	public GtidSet getGtidSet() {
		return gtidSet;
	}

	public void setGtidSet(String gtidSet) {
		if (gtidSet != null && this.binlogFileName == null) {
			this.binlogFileName = "";
		}
		this.gtidSet = gtidSet != null ? new GtidSet(gtidSet) : null;
	}

	protected void setupChecksumState() throws Exception {
		final Query query = new Query(this.transport);

		try {
			List<String> cols = query.getFirst("SELECT @@global.binlog_checksum");

			if (cols != null && (!cols.isEmpty()) && ("CRC32".equals(cols.get(0)) || "NONE".equals(cols.get(0)))) {
				query.getFirst("SET @master_binlog_checksum = @@global.binlog_checksum");
			}
		} catch (TransportException e) {
			// ignore no-such-variable errors on mysql 5.5
			if (e.getErrorCode() != 1193) {
				throw e;
			}
		}
	}

	protected void setupHeartbeatPeriod() throws Exception {
		if (this.heartbeatPeriod == null) {
			return;
		}

		BigInteger nanoSeconds = BigDecimal.valueOf(1000000000).multiply(BigDecimal.valueOf(this.heartbeatPeriod))
				.toBigInteger();

		final Query query = new Query(this.transport);
		query.getFirst("SET @master_heartbeat_period = " + nanoSeconds);
	}

	protected void setupSlaveUUID() throws Exception {
		final Query query = new Query(this.transport);
		query.getFirst("SET @slave_uuid = '" + this.uuid + "'");
	}

	public void setHeartbeatPeriod(float period) {
		this.heartbeatPeriod = period;
	}

	public Float getHeartbeatPeriod() {
		return this.heartbeatPeriod;
	}

	public long getHeartbeatCount() {
		return binlogParser.getHeartbeatCount();
	}

	public Long millisSinceLastEvent() {
		if(this.binlogParser != null) {
			return binlogParser.millisSinceLastEvent();
		} else {
			return -1L;
		}
	}

	public boolean isStopOnEOF() {
		return stopOnEOF;
	}

	public void setStopOnEOF(boolean stopOnEOF) {
		this.stopOnEOF = stopOnEOF;
	}

	public void onStop() {
	}

	protected void dumpBinlog(boolean useGtidIfGtidMode) throws Exception {
		Packet command;
		final Query query = new Query(this.transport);
		if (useGtidIfGtidMode && query.isGtidMode() && this.gtidSet != null) {
			LOG.info(
				String.format(
					"readerTaskName: %s, starting replication at %s",
					this.readerTaskName,
					this.gtidSet.toString()
				)
			);
			command = new ComBinlogDumpGtidPacket(this.binlogPosition, this.serverId, this.binlogFileName,
					this.gtidSet);
		} else {
			LOG.info(String.format(
				"readerTaskName: %s, starting replication at %s:%d",
				this.readerTaskName,
				this.binlogFileName,
				this.binlogPosition
			));
			command = new ComBinlogDumpPacket(this.binlogPosition, this.serverId,
					StringColumn.valueOf(binlogFileName.getBytes(this.encoding)));
		}

		this.transport.getOutputStream().writePacket(command);
		this.transport.getOutputStream().flush();

		//
		final Packet packet = this.transport.getInputStream().readPacket();
		if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
			final ErrorPacket error = ErrorPacket.valueOf(packet);
			throw new TransportException(error);
		}
	}

	protected Transport getDefaultTransport() throws Exception {
		final TransportImpl r = new TransportImpl();
		r.setLevel1BufferSize(this.level1BufferSize);
		r.setLevel2BufferSize(this.level2BufferSize);

		final AuthenticatorImpl authenticator = new AuthenticatorImpl();
		authenticator.setUser(this.rdsUserPassword.getUserName());
		authenticator.setPassword(this.rdsUserPassword.getPassword());
		authenticator.setEncoding(this.encoding);
		r.setAuthenticator(authenticator);

		final SocketFactoryImpl socketFactory = new SocketFactoryImpl();
		socketFactory.setKeepAlive(true);
		socketFactory.setSoTimeout(16);
		socketFactory.setTcpNoDelay(false);
		socketFactory.setReceiveBufferSize(this.socketReceiveBufferSize);
		r.setSocketFactory(socketFactory);
		return r;
	}

	protected ReplicationBasedBinlogParser getDefaultBinlogParser() throws Exception {
		final ReplicationBasedBinlogParser r = new ReplicationBasedBinlogParser(this.readerTaskName, this.stopOnEOF);
		r.registerEventParser(new StopEventParser());
		r.registerEventParser(new RotateEventParser());
		r.registerEventParser(new IntvarEventParser());
		r.registerEventParser(new XidEventParser());
		r.registerEventParser(new RandEventParser());
		r.registerEventParser(new QueryEventParser());
		r.registerEventParser(new UserVarEventParser());
		r.registerEventParser(new IncidentEventParser());
		r.registerEventParser(new TableMapEventParser());
		r.registerEventParser(new WriteRowsEventParser());
		r.registerEventParser(new UpdateRowsEventParser());
		r.registerEventParser(new DeleteRowsEventParser());
		r.registerEventParser(new RowsQueryEventParser());
		r.registerEventParser(new WriteRowsEventV2Parser());
		r.registerEventParser(new UpdateRowsEventV2Parser());
		r.registerEventParser(new DeleteRowsEventV2Parser());
		r.registerEventParser(new FormatDescriptionEventParser());
		r.registerEventParser(new GtidEventParser());

		r.setTransport(this.transport);
		return r;
	}

}
