package com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.error.TransportException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.ErrorPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.GreetingPacket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.ActiveBufferedInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.SocketFactory;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Packet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.TransportInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.TransportOutputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.IOUtils;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransportImpl extends AbstractTransport {
	private static final Logger LOG = LoggerFactory.getLogger(TransportImpl.class);

	protected Socket socket;
	protected TransportInputStream is;
	protected TransportOutputStream os;
	protected SocketFactory socketFactory;
	protected int level1BufferSize = 1024 * 1024;
	protected int level2BufferSize = 8 * 1024 * 1024;
	protected final AtomicBoolean connected = new AtomicBoolean(false);

	@Override public boolean isConnected() {
		return this.connected.get();
	}

	@Override public void connect(String host, int port) throws Exception {
		if (!this.connected.compareAndSet(false, true)) {
			return;
		}

		if (isVerbose() && LOG.isInfoEnabled()) {
			LOG.debug("connecting to host: {}, port: {}", host, port);
		}

		this.socket = this.socketFactory.create(host, port);
		this.os = new TransportOutputStreamImpl(this.socket.getOutputStream());
		if (this.level2BufferSize <= 0) {
			this.is = new TransportInputStreamImpl(this.socket.getInputStream(), this.level1BufferSize);
		} else {
			this.is = new TransportInputStreamImpl(
					new ActiveBufferedInputStream(this.socket.getInputStream(), this.level2BufferSize),
					this.level1BufferSize);
		}

		final Packet packet = this.is.readPacket();
		if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
			final ErrorPacket error = ErrorPacket.valueOf(packet);
			LOG.warn("failed to connect to host: {}, port: {}, error", new Object[] { host, port, error });
			throw new TransportException(error);
		} else {
			final GreetingPacket greeting = GreetingPacket.valueOf(packet);
			this.context.setServerHost(host);
			this.context.setServerPort(port);
			this.context.setServerStatus(greeting.getServerStatus());
			this.context.setServerVersion(greeting.getServerVersion().toString());
			this.context.setServerCollation(greeting.getServerCollation());
			this.context.setServerCapabilities(greeting.getServerCapabilities());
			this.context.setThreadId(greeting.getThreadId());
			this.context.setProtocolVersion(greeting.getProtocolVersion());
			this.context.setScramble(greeting.getScramble1().toString() + greeting.getScramble2().toString());

			if (isVerbose() && LOG.isInfoEnabled()) {
				LOG.debug("connected to host: {}, port: {}, context: {}", new Object[] { host, port, this.context });
			}
		}

		this.authenticator.login(this);
	}

	@Override public void disconnect() throws Exception {
		if (!this.connected.compareAndSet(true, false)) {
			return;
		}

		IOUtils.closeQuietly(this.is);
		IOUtils.closeQuietly(this.os);
		IOUtils.closeQuietly(this.socket);

		if (isVerbose() && LOG.isInfoEnabled()) {
			LOG.debug("disconnected from {}:{}", this.context.getServerHost(), this.context.getServerPort());
		}
	}

	public int getLevel1BufferSize() {
		return level1BufferSize;
	}

	public void setLevel1BufferSize(int size) {
		this.level1BufferSize = size;
	}

	public int getLevel2BufferSize() {
		return level2BufferSize;
	}

	public void setLevel2BufferSize(int size) {
		this.level2BufferSize = size;
	}

	@Override public TransportInputStream getInputStream() {
		return this.is;
	}

	@Override public TransportOutputStream getOutputStream() {
		return this.os;
	}

	public SocketFactory getSocketFactory() {
		return socketFactory;
	}

	public void setSocketFactory(SocketFactory factory) {
		this.socketFactory = factory;
	}
}
