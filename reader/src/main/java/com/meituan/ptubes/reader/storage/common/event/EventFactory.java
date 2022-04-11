/*
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.meituan.ptubes.reader.storage.common.event;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.UnsupportEventVersionRuntimeException;


public abstract class EventFactory {
	public static final byte EVENT_V1 = 1;
	public static final byte EVENT_V2 = 2;

	public static final byte DBUS_EVENT_CURLY_BRACE = 123;
	private static final Logger LOG = LoggerFactory.getLogger("PtubesEventFactory");

	protected EventFactory() {
	}

	public abstract byte getVersion();

	/**
	 * Creates a writable but empty DbusEvent; for internal use by DbusEventBuffer only.
	 * The empty event can be initialized via its reset() method.
	 *
	 * #      [currently used for iterators; expect to switch to createReadOnlyDbusEvent() below in upcoming change]
	 *
	 * @return DbusEventInternalWritable object
	 */
	public abstract EventInternalWritable createWritableDbusEvent();

	public static int serializeEvent(ChangeEntry changeEntry, ByteBuffer serializationBuffer)
			throws PtubesException {
		byte version = changeEntry.getVersion();
		if (version == EVENT_V1) {
			if(SourceType.MySQL.equals(changeEntry.getSourceType())) {
				return PtubesEventV1.serializeEvent((MySQLChangeEntry) changeEntry, serializationBuffer);
			} else {
				LOG.error("Unsupported event version: EVENT_V1, sourceType: " + changeEntry.getSourceType().name());
				throw new UnsupportEventVersionRuntimeException(version);
			}
		} else if (version == EVENT_V2) {
			return PtubesEventV2.serializeEvent(changeEntry, serializationBuffer);
		}
		throw new UnsupportEventVersionRuntimeException(version);
	}

	/**
	 * Creates a writable DbusEvent out of an already initialized (serialized) buffer.
	 * (For DbusEventBuffer and testing ONLY!  hmmm...also now used by SendEventsRequest.ExecHandler)
	 *
	 * @param buf
	 * 		buffer containing the serialized event
	 * @param position
	 * 		byte-offset of the serialized event in the buffer
	 * @return a writable DbusEvent
	 */
	public static EventInternalWritable createWritableEventFromBuffer(ByteBuffer buf, int position) {
		byte version = buf.get(position);
		if (version == EVENT_V1) {
			return new PtubesEventV1(buf, position);
		} else if (version == EVENT_V2) {
			return new PtubesEventV2(buf, position);
		}
		throw new UnsupportEventVersionRuntimeException(version);
	}

	static String getStringFromBuffer(ByteBuffer buf, int position) throws UnsupportedEncodingException {
		byte[] arr;
		int idxFirstPrintable, idxLastPrintable;

		if (buf.hasArray()) {
			arr = buf.array();
			idxFirstPrintable = position + buf.arrayOffset();
		} else // do a copy, sigh
		{
			ByteBuffer roBuf = buf.asReadOnlyBuffer();  // don't want to screw up original buffer's position with get()
			int length = roBuf.position(position).remaining();
			arr = new byte[length];
			roBuf.get(arr);
			idxFirstPrintable = 0;
		}

		assert arr[idxFirstPrintable] == DBUS_EVENT_CURLY_BRACE;
		for (idxLastPrintable = idxFirstPrintable; idxLastPrintable < arr.length; ++idxLastPrintable) {
			byte b = arr[idxLastPrintable];
			// accept only space (32) and above, except DEL (127), plus tab (9), newline (10), and CR (13)
			if (((b < 32) && (b != 9) && (b != 10) && (b != 13)) || (b == 127)) {
				break;
			}
		}
		// idxLastPrintable points either just past the end of the array or at the first unprintable byte
		int length = Math.min(idxLastPrintable - idxFirstPrintable, 512);
		return new String(arr, idxFirstPrintable, length, "UTF-8");  // ASCII is pure subset of UTF-8
	}

	/**
	 * Creates a read-only DbusEvent out of an already initialized (serialized) buffer.
	 *
	 * @param buf
	 * 		buffer containing the serialized event
	 * @param position
	 * 		byte-offset of the serialized event in the buffer
	 * @return a read-only DbusEvent
	 */
	public static EventInternalReadable createReadOnlyEventFromBuffer(ByteBuffer buf, int position) {
		return EventFactory.createWritableEventFromBuffer(buf, position);
	}

	public static int computeEventLength(ChangeEntry changeEntry) {
		if (changeEntry.getVersion() == EVENT_V1) {
			try {
				return PtubesEventV1.getCalculatedEventLength(changeEntry);
			} catch (Exception e) {
				LOG.error("computeEventLength error", e);
				throw new PtubesRunTimeException(e.getMessage());
			}
		} else if (changeEntry.getVersion() == EVENT_V2) {
			try {
				return PtubesEventV2.getCalculatedEventLength(changeEntry);
			} catch (Exception e) {
				LOG.error("computeEventLength error", e);
				throw new PtubesRunTimeException(e.getMessage());
			}
		}
		throw new UnsupportEventVersionRuntimeException(changeEntry.getVersion());
	}

}
