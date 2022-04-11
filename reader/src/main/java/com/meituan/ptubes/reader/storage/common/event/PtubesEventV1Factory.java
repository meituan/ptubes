package com.meituan.ptubes.reader.storage.common.event;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;


public class PtubesEventV1Factory extends EventFactory {
	public static final Logger LOG = LoggerFactory.getLogger("PtubesEventV1Factory");
	private static final byte VERSION = EventFactory.EVENT_V1;

	public PtubesEventV1Factory() {
		if (LOG.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
				sb.append("\n\t").append(ste);
			}
		}
	}

	@Override
	public byte getVersion() {
		return VERSION;
	}

	@Override
	public EventInternalWritable createWritableDbusEvent() {
		return new PtubesEventV1();
	}

}
