package com.meituan.ptubes.reader.container.common.constants;


public class EventBufferConstants {
	public enum QueuePolicy {
		BLOCK_ON_WRITE, OVERWRITE_ON_WRITE
	}

	public enum AllocationPolicy {
		/**Mainly used**/HEAP_MEMORY, DIRECT_MEMORY, MMAPPED_MEMORY
	}

	public enum WindowState {
		INIT, STARTED, EVENTS_ADDED, IN_READ, // State when puller is in readEvents call
		ENDED,
	}

	public enum StreamingMode {
		WINDOW_AT_TIME, // return when a WINDOW worth of events is surved
		CONTINUOUS // as much as fits into buffer
	}

	public enum ReadEventsScanStatus {
		OK, PARTIAL_EVENT, INVALID_EVENT, SCN_REGRESSION, MISSING_EOP
	}
}
