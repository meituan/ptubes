package com.meituan.ptubes.reader.storage.filter;

import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import java.util.Map;

public class SourceAndPartitionEventFilter implements EventFilter {
	private final Map<String, PartitionEventFilter> sources;

	public SourceAndPartitionEventFilter(Map<String, PartitionEventFilter> sources) {
		this.sources = sources;
	}

	@Override
	public boolean allow(PtubesEvent event) {
		if (EventType.SENTINEL == event.getEventType()) {
			return false;
		}
		if (EventType.isErrorEvent(event.getEventType())) {
			return true;
		}
		String tableName = event.getTableName();
		EventType eventType = event.getEventType();
		
		if (EventType.isBroadcastType(eventType)) {
			return true;
		}
		PartitionEventFilter partitionEventFilter = sources.get(tableName);
		if (null == partitionEventFilter) {
			return false;
		} else {
			return partitionEventFilter.allow(event);
		}
	}

}
