package com.meituan.ptubes.reader.storage.mem.buffer.index;

import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;

public interface InternalEventsListener {
	void onEvent(PtubesEvent event, long offset, int size);

	void onTxn(BinlogInfo binlogInfo, long offset);

	void close() throws IOException;
}
