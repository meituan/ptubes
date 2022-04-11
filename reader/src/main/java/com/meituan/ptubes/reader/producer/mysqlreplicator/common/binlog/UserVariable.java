package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog;

public interface UserVariable {

	int getType();

	Object getValue();
}
