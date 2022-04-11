package com.meituan.ptubes.reader.storage.mem.buffer;

import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;

public interface EventBufferAppendable {
	void start(BinlogInfo binlogInfo);

	/**
	 * Notify that the producer will be starting to append events for the next window
	 */
	void startEvents();

	int appendEvent(ChangeEntry changeEntry);

	void rollbackEvents();

	boolean empty();

	BinlogInfo getMinBinlogInfo();

	BinlogInfo getLastWrittenBinlogInfo();

	void setStartBinlogInfo(BinlogInfo binlogInfo);
}
