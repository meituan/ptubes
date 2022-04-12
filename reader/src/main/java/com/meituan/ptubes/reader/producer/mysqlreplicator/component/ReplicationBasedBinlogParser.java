package com.meituan.ptubes.reader.producer.mysqlreplicator.component;

import com.meituan.ptubes.reader.container.common.constants.ProducerConstants;
import com.meituan.ptubes.reader.container.common.vo.GtidSet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.BinlogEventV4HeaderImpl;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.EOFPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.ErrorPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.OKPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Transport;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.TransportInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.AbstractBinlogParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.EventInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import org.apache.logging.log4j.ThreadContext;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ReplicationBasedBinlogParser extends AbstractBinlogParser {
	private static final Logger LOG = LoggerFactory.getLogger(ReplicationBasedBinlogParser.class);
	private final boolean stopOnEOF;
	private final String readerTaskName;
	private final String bigEventCatType;
	protected long heartbeatCount = 0;
	protected Long lastEventMillis = null;

	protected Transport transport;

	public ReplicationBasedBinlogParser(String readerTaskName, boolean stopOnEOF) {
		this.readerTaskName = readerTaskName;
		this.bigEventCatType = ProducerConstants.CAT_BIG_EVENT_TYPE_PRE;
		this.stopOnEOF = stopOnEOF;
	}

	@Override
	protected void doStart() throws Exception {
		// NOP
	}

	@Override
	protected void doStop(long timeout, TimeUnit unit) throws Exception {
		// NOP
	}

	@Override
	protected void doStop() throws Exception {
	}

	public Transport getTransport() {
		return transport;
	}

	public void setTransport(Transport transport) {
		this.transport = transport;
	}

	@Override
	public String getBinlogFileName() {
		return binlogFileName;
	}

	public void setBinlogFileName(String binlogFileName) {
		this.binlogFileName = binlogFileName;
	}

	public void setPrevGtidSet(GtidSet prevGtidSet) {
		this.prevGtidSet = prevGtidSet;
	}

	private void readPacketMarker(TransportInputStream ts) throws IOException {
		final int packetMarker = ts.readInt(1);
		if (packetMarker != OKPacket.PACKET_MARKER) {
			if ((byte) packetMarker == ErrorPacket.PACKET_MARKER) {
				final ErrorPacket packet = ErrorPacket.valueOf(ts.currentPacketLength(), ts.currentPacketSequence(),
						packetMarker, ts);
				throw new RuntimeException(packet.toString());
			} else if ((byte) packetMarker == EOFPacket.PACKET_MARKER) {
				if (stopOnEOF) {
					throw new EOFException();
				} else {
					final EOFPacket packet = EOFPacket.valueOf(ts.currentPacketLength(), ts.currentPacketSequence(),
							packetMarker, ts);
					throw new RuntimeException(packet.toString());
				}
			} else {
				throw new RuntimeException("assertion failed, invalid packet marker: " + packetMarker);
			}
		}
	}

	public long getHeartbeatCount() {
		return this.heartbeatCount;
	}

	public Long millisSinceLastEvent() {
		if (this.lastEventMillis == null) {
			return null;
		}

		return System.currentTimeMillis() - this.lastEventMillis;
	}

	@Override
	protected void doParse() throws Exception {
		ThreadContext.put("TASKNAME", readerTaskName);
		final TransportInputStream is = this.transport.getInputStream();
		final EventInputStream es = new EventInputStream(is);
		context = new Context(this, context);

		try {
			BinlogEventV4HeaderImpl header;
			while (isRunning()) {
				readPacketMarker(is);
				header = es.getNextBinlogHeader();

				boolean isFormatDescriptionEvent = header.getEventType() == MySQLConstants.FORMAT_DESCRIPTION_EVENT;

				if (header.getEventType() == MySQLConstants.HEARTBEAT_LOG_EVENT) {
					this.heartbeatCount++;
				}

				this.lastEventMillis = System.currentTimeMillis();

				// Parse the event body
				if (this.eventFilter != null && !this.eventFilter.accepts(header, context)) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Skip event, header: " + header);
					}
					switch (header.getEventType()) {
					/*
					FORMAT_DESCRIPTION events must always be parsed to ensure that we record
					checksum info -- if the caller has filtered them out,
					we still need to know.
				 	*/
						case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
						case MySQLConstants.GTID_LOG_EVENT:
							BinlogEventParser parser = getEventParser(header.getEventType());
							parser.parse(es, header, context);
							break;
						default:
							this.defaultParser.parse(es, header, context);
					}
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Receive event, header: " + header);
					}

					reportBigEvent(header);

					BinlogEventParser parser = getEventParser(header.getEventType());
					parser.parse(es, header, context);
				}

				if (isFormatDescriptionEvent) {
					es.setChecksumEnabled(context.getChecksumEnabled());
				}

				es.finishEvent(header);
			}
		} finally {
			es.close();
		}
	}

	private void reportBigEvent(BinlogEventV4HeaderImpl header) {
		if (LOG.isDebugEnabled()) {
			if (MySQLConstants.HEARTBEAT_LOG_EVENT == header.getEventType()) {
				return;
			}
			long eventSize = header.getNextPosition() - header.getPosition();
			LOG.debug("{} handle an event of size {}B", readerTaskName, eventSize);
		}
	}
}
