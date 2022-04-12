package com.meituan.ptubes.reader.storage.common.event;

import java.io.IOException;


public interface InternalEventsListener {
	public void onEvent(PtubesEvent event, long offset, int size);

	public void close() throws IOException;
}
