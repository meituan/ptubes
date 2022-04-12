package com.meituan.ptubes.reader.storage.mem.buffer.index;

import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;

public abstract class InternalEventsListenerAbstract implements InternalEventsListener {
	@Override
	public void onEvent(PtubesEvent event, long offset, int size) {
	}

	@Override
	public void onTxn(BinlogInfo binlogInfo, long offset) {
	}

	@Override
	public void close() throws IOException {
	}
}
