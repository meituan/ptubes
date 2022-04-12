package com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl;

import java.net.Socket;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.SocketFactory;

public class SocketFactoryImpl implements SocketFactory {
	private boolean keepAlive = false;
	private int soTimeout = 16;
	private boolean tcpNoDelay = false;
	private int receiveBufferSize = -1;

	@Override public Socket create(String host, int port) throws Exception {
		final Socket r = new Socket(host, port);
		r.setKeepAlive(this.keepAlive);
		r.setSoTimeout(soTimeout * 1000);
		r.setTcpNoDelay(this.tcpNoDelay);
		if (this.receiveBufferSize > 0) {
			r.setReceiveBufferSize(this.receiveBufferSize);
		}
		return r;
	}

	public boolean isKeepAlive() {
		return keepAlive;
	}

	public void setKeepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
	}

	public int getSoTimeout() {
		return soTimeout;
	}

	public void setSoTimeout(int soTimeout) {
		this.soTimeout = soTimeout;
	}

	public boolean isTcpNoDelay() {
		return tcpNoDelay;
	}

	public void setTcpNoDelay(boolean tcpNoDelay) {
		this.tcpNoDelay = tcpNoDelay;
	}

	public int getReceiveBufferSize() {
		return receiveBufferSize;
	}

	public void setReceiveBufferSize(int receiveBufferSize) {
		this.receiveBufferSize = receiveBufferSize;
	}
}
