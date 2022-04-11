package com.meituan.ptubes.reader.container.common.config.producer;


public class TableConfig {
	private String databaseName;
	private String tableName;
	private String partitionKey;

	public TableConfig() {

	}

	public TableConfig(String databaseName, String tableName, String partitionKey) {
		this.databaseName = databaseName;
		this.tableName = tableName;
		this.partitionKey = partitionKey;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getPartitionKey() {
		return partitionKey;
	}

	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}
}
