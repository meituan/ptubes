package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog;

public interface BinlogParserListener {

	void onStart(BinlogParser parser);

	void onStop(BinlogParser parser);

	void onException(BinlogParser parser, Exception eception);

	class Adapter implements BinlogParserListener {

		@Override public void onStart(BinlogParser parser) {
		}

		@Override public void onStop(BinlogParser parser) {
		}

		@Override public void onException(BinlogParser parser, Exception exception) {
		}
	}
}
