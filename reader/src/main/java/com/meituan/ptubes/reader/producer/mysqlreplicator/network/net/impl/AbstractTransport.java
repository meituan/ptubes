package com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl;

import java.util.concurrent.atomic.AtomicBoolean;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Transport;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.TransportContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public abstract class AbstractTransport implements Transport {
	//
	protected Transport.Authenticator authenticator;
	protected final Context context = new Context();
	protected final AtomicBoolean verbose = new AtomicBoolean(true);

	public boolean isVerbose() {
		return this.verbose.get();
	}

	public void setVerbose(boolean verbose) {
		this.verbose.set(verbose);
	}

	@Override public TransportContext getContext() {
		return context;
	}

	public Transport.Authenticator getAuthenticator() {
		return authenticator;
	}

	public void setAuthenticator(Transport.Authenticator authenticator) {
		this.authenticator = authenticator;
	}

	public static class Context implements TransportContext {
		//
		private long threadId;
		private String scramble;
		private int protocolVersion;
		private String serverHost;
		private int serverPort;
		private int serverStatus;
		private int serverCollation;
		private String serverVersion;
		private int serverCapabilities;

		/**
		 *
		 */
		@Override
		public String toString() {
			return new ToStringBuilder(this).append("threadId", threadId).append("scramble", scramble).append(
					"protocolVersion", protocolVersion).append("serverHost", serverHost).append("serverPort",
					serverPort).append("serverStatus", serverStatus).append("serverCollation", serverCollation).append(
					"serverVersion", serverVersion).append("serverCapabilities", serverCapabilities).toString();
		}

		/**
		 *
		 */
		@Override
        public long getThreadId() {
			return threadId;
		}

		public void setThreadId(long threadId) {
			this.threadId = threadId;
		}

		@Override
        public String getScramble() {
			return scramble;
		}

		public void setScramble(String scramble) {
			this.scramble = scramble;
		}

		@Override
        public int getProtocolVersion() {
			return protocolVersion;
		}

		public void setProtocolVersion(int protocolVersion) {
			this.protocolVersion = protocolVersion;
		}

		@Override
        public String getServerHost() {
			return serverHost;
		}

		public void setServerHost(String host) {
			this.serverHost = host;
		}

		@Override
        public int getServerPort() {
			return serverPort;
		}

		public void setServerPort(int port) {
			this.serverPort = port;
		}

		@Override
        public int getServerStatus() {
			return serverStatus;
		}

		public void setServerStatus(int serverStatus) {
			this.serverStatus = serverStatus;
		}

		@Override
        public int getServerCollation() {
			return serverCollation;
		}

		public void setServerCollation(int serverCollation) {
			this.serverCollation = serverCollation;
		}

		@Override
        public String getServerVersion() {
			return serverVersion;
		}

		public void setServerVersion(String serverVersion) {
			this.serverVersion = serverVersion;
		}

		@Override
        public int getServerCapabilities() {
			return serverCapabilities;
		}

		public void setServerCapabilities(int serverCapabilities) {
			this.serverCapabilities = serverCapabilities;
		}
	}
}
