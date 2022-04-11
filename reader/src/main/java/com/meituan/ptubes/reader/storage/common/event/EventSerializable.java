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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class EventSerializable extends EventInternalWritable {
	private static final TypeReference<Map<String, Object>> JSON_GENERIC_MAP_TYPEREF = new TypeReference<Map<String, Object>>() {
	};
	public static final String MODULE = EventSerializable.class.getName();
	public static final Logger LOG = LogManager.getLogger(MODULE);

	// Note that buf and position are reflectorized in DbusEventCorrupter,
	// so changes here (renames, moves) must be reflected there.
	protected ByteBuffer buf;
	protected int position;
	protected boolean inited;

	@Override
	public HeaderScanStatus scanHeader() {
		return scanHeader(true);
	}

	@Override
	public EventScanStatus scanEvent() {
		return scanEvent(true);
	}

	@Override
	public boolean isValid() {
		return isValid(true);
	}

	/**
	 * @param logErrors
	 * 		whether to emit LOG.error messages for invalid results
	 * @return true if event is not partial and event is valid; note that a partial event is deemed invalid
	 */
	@Override
	public boolean isValid(boolean logErrors) {
		return (buf != null) && (scanEvent(logErrors) == EventScanStatus.OK);
	}

	@Override
	public ByteBuffer getRawBytes() {
		ByteBuffer buffer = buf.asReadOnlyBuffer().order(buf.order());
		buffer.position(position);
		buffer.limit(position + size());
		return buffer;
	}

	/**
	 * Serializes the event to a channel using the specified encoding
	 *
	 * @param writeChannel
	 * 		the channel to write to
	 * @param encoding
	 * 		the serialization encoding
	 * @return the number of bytes written to the channel
	 */
	@Override
	public int writeTo(WritableByteChannel writeChannel, Encoding encoding) {
		int bytesWritten = 0;
		switch (encoding) {
			case BINARY: {
				// write a copy of the event
				ByteBuffer writeBuffer = buf.duplicate().order(buf.order());
				writeBuffer.position(position);
				writeBuffer.limit(position + size());
				try {
					bytesWritten = writeChannel.write(writeBuffer);
				} catch (IOException e) {
					LOG.error("binary write error: " + e.getMessage(), e);
				}
				break;
			}
			case JSON_PLAIN_VALUE:
			case JSON: {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				JsonFactory f = new JsonFactory();
				try {
					JsonGenerator g = f.createJsonGenerator(baos, JsonEncoding.UTF8);
					g.writeStartObject();
					int version = getVersion();
					if (version == EventFactory.EVENT_V1) {
						writeJSON_V1(g, encoding);
					} else {
						writeJSON_V2(g, encoding);
					}

					g.writeEndObject();
					g.flush();
					baos.write("\n".getBytes(Charset.defaultCharset()));
				} catch (IOException e) {
					LOG.error("JSON write error: " + e.getMessage(), e);
				}
				ByteBuffer writeBuffer = ByteBuffer.wrap(baos.toByteArray()).order(buf.order());
				try {
					bytesWritten = writeChannel.write(writeBuffer);
				} catch (IOException e) {
					LOG.error("JSON write error: " + e.getMessage(), e);
				}
				break;
			}
			default:
		}
		return bytesWritten;
	}

	private void writeJSON_V1(JsonGenerator g, Encoding e) throws IOException {
		// unsupport
		return;
	}

	private void writeJSON_V2(JsonGenerator g, Encoding e) throws IOException {
		// unsupport
		return;
	}

	protected void resetInternal(ByteBuffer buf, int position) {
		verifyByteOrderConsistency(buf, "DbusEventSerializable.resetInternal()");
		inited = true;
		this.buf = buf;
		this.position = position;
	}

	protected void verifyByteOrderConsistency(ByteBuffer buf, String where) {
		if (inited && (buf.order() != this.buf.order())) {
			throw new PtubesRunTimeException(
					"ByteBuffer byte-order mismatch [" + (where != null ? where : "verifyByteOrderConsistency()")
							+ "]");
		}
	}

}
