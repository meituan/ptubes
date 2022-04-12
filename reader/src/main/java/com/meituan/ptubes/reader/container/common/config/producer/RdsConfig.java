package com.meituan.ptubes.reader.container.common.config.producer;


public class RdsConfig {
	private String ip;
	private int port;
	private String userName;
	private String password;
	private long serverId;

	public RdsConfig() {
	}

	// for test
	public RdsConfig(String ip, int port, String userName, String password, long serverId) {
		this.ip = ip;
		this.port = port;
		this.userName = userName;
		this.password = password;
		this.serverId = serverId;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public long getServerId() {
		return serverId;
	}

	public void setServerId(long serverId) {
		this.serverId = serverId;
	}

	@Override
	public String toString() {
		return "RdsConfig{" +
				"ip='" + ip + '\'' +
				", port=" + port +
				", userName='" + userName + '\'' +
				", password='" + password + '\'' +
				", serverId=" + serverId +
				'}';
	}
}
