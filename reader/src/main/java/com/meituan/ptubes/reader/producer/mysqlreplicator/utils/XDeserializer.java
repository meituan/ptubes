package com.meituan.ptubes.reader.producer.mysqlreplicator.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.BitColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStreamImpl;

public class XDeserializer implements XInputStream {
	private final XInputStream tis;

	public XDeserializer(byte[] data) {
		this.tis = new XInputStreamImpl(new ByteArrayInputStream(data));
	}

	@Override
	public void close() throws IOException {
		this.tis.close();
	}

	@Override
	public int available() throws IOException {
		return this.tis.available();
	}

	@Override
	public boolean hasMore() throws IOException {
		return this.tis.hasMore();
	}

	@Override
	public void setReadLimit(int limit) throws IOException {
		this.tis.setReadLimit(limit);
	}

	@Override
	public long skip(long n) throws IOException {
		return this.tis.skip(n);
	}

	@Override public int readInt(int length) throws IOException {
		return this.tis.readInt(length);
	}

	@Override public long readLong(int length) throws IOException {
		return this.tis.readLong(length);
	}

	@Override public byte[] readBytes(int length) throws IOException {
		return this.tis.readBytes(length);
	}

	@Override public BitColumn readBit(int length) throws IOException {
		return readBit(length);
	}

	@Override public int readSignedInt(int length) throws IOException {
		return this.tis.readSignedInt(length);
	}

	@Override public long readSignedLong(int length) throws IOException {
		return this.tis.readSignedLong(length);
	}

	@Override public UnsignedLong readUnsignedLong() throws IOException {
		return tis.readUnsignedLong();
	}

	@Override public StringColumn readLengthCodedString() throws IOException {
		return this.tis.readLengthCodedString();
	}

	@Override public StringColumn readNullTerminatedString() throws IOException {
		return this.tis.readNullTerminatedString();
	}

	@Override public StringColumn readFixedLengthString(int length) throws IOException {
		return this.tis.readFixedLengthString(length);
	}

	@Override public StringColumn readFixedLengthJsonString(int length) throws IOException {
		return this.tis.readFixedLengthJsonString(length);
	}

	@Override public int readInt(int length, boolean littleEndian) throws IOException {
		return this.tis.readInt(length, littleEndian);
	}

	@Override public long readLong(int length, boolean littleEndian) throws IOException {
		return this.tis.readLong(length, littleEndian);
	}

	@Override public BitColumn readBit(int length, boolean littleEndian) throws IOException {
		return tis.readBit(length, littleEndian);
	}

	@Override public int read(byte[] b, int off, int len) throws IOException {
		return tis.read(b, off, len);
	}

	@Override public int read() throws IOException {
		return tis.read();
	}
}
