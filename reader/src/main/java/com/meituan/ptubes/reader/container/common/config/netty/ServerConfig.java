package com.meituan.ptubes.reader.container.common.config.netty;

/**
 * Server-side configuration information
 */
public class ServerConfig {
	private int dataPort;
	private int monitorPort;
	private int maxInitialLineLength;
	private int maxHeaderSize;
	private int maxChunkSize;
	private int maxContentLength;
	private int writeBufferLowWaterMark;
	private int WriteBufferHighWaterMark;

	public ServerConfig(int dataPort, int monitorPort, int maxInitialLineLength, int maxHeaderSize,
			int maxChunkSize, int maxContentLength, int writeBufferLowWaterMark, int writeBufferHighWaterMark) {
		this.dataPort = dataPort;
		this.monitorPort = monitorPort;
		this.maxInitialLineLength = maxInitialLineLength;
		this.maxHeaderSize = maxHeaderSize;
		this.maxChunkSize = maxChunkSize;
		this.maxContentLength = maxContentLength;
		this.writeBufferLowWaterMark = writeBufferLowWaterMark;
		WriteBufferHighWaterMark = writeBufferHighWaterMark;
	}

	public int getDataPort() {
		return dataPort;
	}

	public void setDataPort(int dataPort) {
		this.dataPort = dataPort;
	}

	public int getMonitorPort() {
		return monitorPort;
	}

	public void setMonitorPort(int monitorPort) {
		this.monitorPort = monitorPort;
	}

	public int getMaxInitialLineLength() {
		return maxInitialLineLength;
	}

	public void setMaxInitialLineLength(int maxInitialLineLength) {
		this.maxInitialLineLength = maxInitialLineLength;
	}

	public int getMaxHeaderSize() {
		return maxHeaderSize;
	}

	public void setMaxHeaderSize(int maxHeaderSize) {
		this.maxHeaderSize = maxHeaderSize;
	}

	public int getMaxChunkSize() {
		return maxChunkSize;
	}

	public void setMaxChunkSize(int maxChunkSize) {
		this.maxChunkSize = maxChunkSize;
	}

	public int getMaxContentLength() {
		return maxContentLength;
	}

	public void setMaxContentLength(int maxContentLength) {
		this.maxContentLength = maxContentLength;
	}

	public int getWriteBufferLowWaterMark() {
		return writeBufferLowWaterMark;
	}

	public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
		this.writeBufferLowWaterMark = writeBufferLowWaterMark;
	}

	public int getWriteBufferHighWaterMark() {
		return WriteBufferHighWaterMark;
	}

	public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
		WriteBufferHighWaterMark = writeBufferHighWaterMark;
	}
}
