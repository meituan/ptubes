package com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.error.CRCException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.BinlogEventV4HeaderImpl;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStreamImpl;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;

public class EventInputStream extends XInputStreamImpl implements XInputStream {
	private final XInputStream dataStream;
	private boolean checksumEnabled = false;
	private CRC32 crc = null;

	public EventInputStream(XInputStream is) {
		super((InputStream) is);
		this.dataStream = is;
	}

	public BinlogEventV4HeaderImpl getNextBinlogHeader() throws IOException {
		final BinlogEventV4HeaderImpl header = new BinlogEventV4HeaderImpl();

		this.setReadLimit(0);

		if (isChecksumEnabled()) {
			crc = new CRC32();
		} else {
			crc = null;
		}

		header.setTimestamp(readLong(4) * 1000L);
		header.setEventType(readInt(1));
		header.setServerId(readLong(4));
		header.setEventLength(readInt(4));

		// setup the total event length; this is different than setReadLimit(),
		// as setReadLimit refers to *packet* length.
		long eventLimit = header.getEventLength() - 13;
		if (isChecksumEnabled() && header.getEventType() != MySQLConstants.FORMAT_DESCRIPTION_EVENT) {
			eventLimit -= 4;
		}

		// TODO fixme re: int overflow
		this.setReadLimit((int) eventLimit);

		header.setNextPosition(readLong(4));
		header.setFlags(readInt(2));
		header.setReplicatorInboundTS(System.currentTimeMillis());

		return header;
	}

	public boolean isChecksumEnabled() {
		return checksumEnabled;
	}

	public void setChecksumEnabled(boolean checksumEnabled) {
		this.checksumEnabled = checksumEnabled;
	}

	public void finishEvent(BinlogEventV4HeaderImpl header) throws IOException {
		// Ensure the packet boundary
		if (this.available() != 0) {
			throw new RuntimeException(
					"assertion failed!  We left " + this.available() + " unconsumed bytes in the buffer for event: "
							+ header);
		}

		if (isChecksumEnabled() && header.getEventType() != MySQLConstants.FORMAT_DESCRIPTION_EVENT) {
			long calculatedCRC = crc.getValue();
			this.setReadLimit(0);
			Long checksum = this.readLong(4);
			if (checksum.longValue() != calculatedCRC) {
				throw new CRCException(header);
			}
		}
	}

	@Override
	public long skip(final long n) throws IOException {
		if (n <= 0) {
			return 0;
		}
		if (!isChecksumEnabled()) {
			this.readCount += n;
			return dataStream.skip(n);
		} else {
			byte[] b = new byte[(int) n];
			// let read calculate the CRC
			read(b, 0, (int) n);
		}
		return n;
	}

	@Override
	public int read(final byte[] b, int off, final int len) throws IOException {
		this.readCount += len;
		int ret = dataStream.read(b, off, len);

		if (isChecksumEnabled() && crc != null) {
			crc.update(b, off, len);
		}

		return ret;
	}

	@Override
	public int read() throws IOException {
		int b = dataStream.read();
		this.readCount++;
		if (isChecksumEnabled() && crc != null) {
			crc.update(b);
		}

		return b;
	}
}
