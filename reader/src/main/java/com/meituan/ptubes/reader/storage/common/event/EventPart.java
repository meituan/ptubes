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

import java.nio.ByteBuffer;
import java.util.Arrays;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import org.apache.commons.codec.binary.Hex;


public class EventPart {
	private static final short SCHEMA_DIGEST_TYPE_MD5 = 0;
	private static final short SCHEMA_DIGEST_TYPE_CRC32 = 1;
	private static final short VERSION_SHIFT = 2;
	private static final short DIGEST_MASK = 0x3;
	private static final int ATTRIBUTES_OFFSET = 4;
	private static final int ATTRIBUTES_LEN = 2;
	private static final int MAX_DATA_BYTES_PRINTED = 64;

	private final PtubesEvent.SchemaDigestType schemaDigestType;
	private final byte[] schemaDigest;
	private final short schemaVersion;
	private ByteBuffer data;

	public EventPart(PtubesEvent.SchemaDigestType schemaDigestType, byte[] schemaDigest, short schemaVersion, ByteBuffer data) {
		this.schemaDigestType = schemaDigestType;
		this.schemaDigest = schemaDigest.clone();
		this.schemaVersion = schemaVersion;
		this.data = data;
		switch (this.schemaDigestType) {
		case MD5:
			if (this.schemaDigest.length != PtubesEvent.MD5_DIGEST_LEN) {
				throw new PtubesRunTimeException("Invalid MD5 schema digest length:" + this.schemaDigest.length);
			}
			break;
		case CRC32:
			if (this.schemaDigest.length != PtubesEvent.CRC32_DIGEST_LEN) {
				throw new PtubesRunTimeException("Invalid CRC-32 schema digest length:" + this.schemaDigest.length);
			}
			break;
		default:
			throw new UnsupportedOperationException("Unsupported schema digest type:" + this.schemaDigestType);
		}
	}

	public PtubesEvent.SchemaDigestType getSchemaDigestType() {
		return schemaDigestType;
	}

	public byte[] getSchemaDigest() {
		return schemaDigest.clone();
	}

	public short getSchemaVersion() {
		return schemaVersion;
	}

	/**
	 * Returns the actual data blob of the event-part.  The returned ByteBuffer is
	 * read-only, and its position and limit may be freely modified.
	 * <b>NOTE: The data may be subsequently overwritten; if you need it beyond the
	 * onDataEvent() call, save your own copy before returning.</b>
	 */
	public ByteBuffer getData() {
		return data.asReadOnlyBuffer().slice().order(data.order());
	}

	/** Returns number of bytes remaining. */
	private int getDataLength() {
		return data.limit() - data
				.position();
	}

	public void encode(ByteBuffer buf) {
		int curPos = data.position();
		buf.putInt(getDataLength());
		short attributes = 0;
		switch (schemaDigestType) {
		case MD5:
			attributes = SCHEMA_DIGEST_TYPE_MD5;
			break;
		case CRC32:
			attributes = SCHEMA_DIGEST_TYPE_CRC32;
			break;
		default:
			throw new UnsupportedOperationException("Unsupported schema digest type:" + schemaDigestType);
		}
		attributes |= (schemaVersion << VERSION_SHIFT);
		buf.putShort(attributes);
		buf.put(schemaDigest);
		buf.put(data);
		data.position(curPos);
	}

	private static PtubesEvent.SchemaDigestType digestType(short attributes) {
		short digestType = (short) (attributes & DIGEST_MASK);
		switch (digestType) {
		case SCHEMA_DIGEST_TYPE_CRC32:
			return PtubesEvent.SchemaDigestType.CRC32;
		case SCHEMA_DIGEST_TYPE_MD5:
			return PtubesEvent.SchemaDigestType.MD5;
		default:
			throw new UnsupportedOperationException("Digest type " + digestType + " not supported");
		}
	}

	private static int digestLen(PtubesEvent.SchemaDigestType digestType) {
		switch (digestType) {
		case MD5:
			return PtubesEvent.MD5_DIGEST_LEN;
		case CRC32:
			return PtubesEvent.CRC32_DIGEST_LEN;
		default:
			throw new UnsupportedOperationException("Digest type " + digestType + " not supported");
		}
	}

	/**
	 * Decodes a bytebuffer and returns DbusEventPart. Preserves the ByteBuffer position.
	 *
	 * @param buf
	 * @return
	 */
	public static EventPart decode(ByteBuffer buf) {
		int pos = buf.position();
		int dataLen = buf.getInt(pos);
		if (dataLen < 0) {
			throw new UnsupportedOperationException("Data length " + dataLen + " not supported");
		}
		short attributes = buf.getShort(pos + ATTRIBUTES_OFFSET);
		short schemaVersion = (short) (attributes >> VERSION_SHIFT);
		PtubesEvent.SchemaDigestType schemaDigestType = digestType(attributes);
		int digestLen = digestLen(schemaDigestType);
		byte[] digest = new byte[digestLen];
		for (int i = 0; i < digestLen; i++) {
			digest[i] = buf.get(pos + ATTRIBUTES_OFFSET + ATTRIBUTES_LEN + i);
		}

		// NOTE - this will create a new ByteBuffer object pointing to the
		// same memory. So the position of this new BB is beginning of the data
		// and limit is set to the end of the data.
		ByteBuffer dataBuf = buf.asReadOnlyBuffer();
		dataBuf.position(pos + ATTRIBUTES_OFFSET + ATTRIBUTES_LEN + digestLen);
		dataBuf.limit(dataBuf.position() + dataLen);

		return new EventPart(schemaDigestType, digest, schemaVersion, dataBuf);
	}

	public static int computePartLength(PtubesEvent.SchemaDigestType digestType, int dataLen) {
		return ATTRIBUTES_OFFSET + ATTRIBUTES_LEN + dataLen + digestLen(digestType);
	}

	/**
	 * Replace the schema-digest in a serialized DbusEventPart.
	 *
	 * @param buf
	 * 		The buffer that contains the serialized DbusEventPart.
	 * @param position
	 * 		the position in the buffer where the DbusEventPart starts.
	 * @param schemaDigest
	 * 		The digest value to substitute. The value must match in length to the existing value.
	 */
	public static void replaceSchemaDigest(ByteBuffer buf, int position, byte[] schemaDigest) {
		PtubesEvent.SchemaDigestType digestType = digestType(buf.getShort(position + ATTRIBUTES_OFFSET));
		int digestLen = digestLen(digestType);
		if (schemaDigest.length != digestLen) {
			throw new RuntimeException(
					"Expecting length " + digestLen + " for type " + digestType + ", found " + schemaDigest.length);
		}
		for (int i = 0; i < digestLen; i++) {
			buf.put(position + ATTRIBUTES_OFFSET + ATTRIBUTES_LEN + i, schemaDigest[i]);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(64);
		sb.append("Length=").append(getDataLength()).append(";SchemaVersion=").append(getSchemaVersion()).append(
				";SchemaId=").append("0x").append(Hex.encodeHexString(getSchemaDigest()));

		ByteBuffer dataBB = getData();
		if (dataBB.remaining() > MAX_DATA_BYTES_PRINTED) {
			dataBB.limit(MAX_DATA_BYTES_PRINTED);
		}
		byte[] data = new byte[dataBB.remaining()];
		dataBB.get(data);
		sb.append(";Value=").append("0x").append(Hex.encodeHexString(data));
		return sb.toString();
	}

	/**
	 * @return The number of bytes that this DbusEventPart would take up, if it were to be serialized.
	 */
	public int computePartLength() {
		return ATTRIBUTES_OFFSET + ATTRIBUTES_LEN + data.remaining() + schemaDigest.length;
	}

	/**
	 * @return the length of the DbusEventPart that is encoded in 'buf' at position 'position'.
	 * Callers can use this method to advance across the DbusEventPart in a serialized V2 event.
	 */
	public static int partLength(ByteBuffer buf, int position) {
		PtubesEvent.SchemaDigestType digestType = digestType(buf.getShort(position + ATTRIBUTES_OFFSET));
		int digestLen = digestLen(digestType);
		return ATTRIBUTES_OFFSET + ATTRIBUTES_LEN + digestLen + buf.getInt(position);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		EventPart other = (EventPart) obj;
		if (schemaVersion != other.schemaVersion) {
			return false;
		}
		if (schemaDigestType != other.schemaDigestType) {
			return false;
		}
		if (schemaDigest == null && other.schemaDigest != null
				|| schemaDigest != null && other.schemaDigest == null) {
			return false;
		}
		if (!Arrays.equals(schemaDigest, other.schemaDigest)) {
			return false;
		}

		if (data == null && other.data != null) {
			return false;
		}
		if (data != null && !data.equals(other.data)) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (schemaVersion);
		if(data != null) {
			result = prime * result + data.hashCode();
		}
		return result;
	}
}
