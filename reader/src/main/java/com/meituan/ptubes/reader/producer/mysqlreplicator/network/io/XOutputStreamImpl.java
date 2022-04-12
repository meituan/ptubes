/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.meituan.ptubes.reader.producer.mysqlreplicator.network.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;

public class XOutputStreamImpl extends BufferedOutputStream implements XOutputStream {

	public XOutputStreamImpl(OutputStream out) {
		super(out);
	}

	@Override public final void writeBytes(byte[] value) throws IOException {
		super.write(value, 0, value.length);
	}

	@Override public final void writeBytes(int value, int length) throws IOException {
		for (int i = 0; i < length; i++) {
			super.write(value);
		}
	}

	@Override public final void writeBytes(byte[] value, int offset, int length) throws IOException {
		super.write(value, offset, length);
	}

	@Override public final void writeInt(int value, int length) throws IOException {
		for (int i = 0; i < length; i++) {
			super.write(0x000000FF & (value >>> (i << 3)));
		}
	}

	@Override public final void writeLong(long value, int length) throws IOException {
		for (int i = 0; i < length; i++) {
			super.write((int) (0x00000000000000FF & (value >>> (i << 3))));
		}
	}

	@Override public final void writeUnsignedLong(UnsignedLong value) throws IOException {
		final long length = value.longValue();
		if (length < 0) {
			writeLong(254, 1);
			writeLong(length, 8);
		} else if (length < 251L) {
			writeLong(length, 1);
		} else if (length < 65536L) {
			writeLong(252, 1);
			writeLong(length, 2);
		} else if (length < 16777216L) {
			writeLong(253, 1);
			writeLong(length, 3);
		} else {
			writeLong(254, 1);
			writeLong(length, 8);
		}
	}

	@Override public final void writeLengthCodedString(StringColumn value) throws IOException {
		writeUnsignedLong(UnsignedLong.valueOf(value.getValue().length));
		writeFixedLengthString(value);
	}

	@Override public final void writeFixedLengthString(StringColumn value) throws IOException {
		super.write(value.getValue());
	}

	@Override public final void writeNullTerminatedString(StringColumn value) throws IOException {
		super.write(value.getValue());
		super.write(0);
	}
}
