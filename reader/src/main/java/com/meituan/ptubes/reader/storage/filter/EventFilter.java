package com.meituan.ptubes.reader.storage.filter;

import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;

public interface EventFilter {
	boolean allow(PtubesEvent event);
}
