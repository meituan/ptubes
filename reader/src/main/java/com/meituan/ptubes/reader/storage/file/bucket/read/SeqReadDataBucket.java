package com.meituan.ptubes.reader.storage.file.bucket.read;

import com.meituan.ptubes.common.utils.GZipUtil;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * read data sequentially bucket
 */
public final class SeqReadDataBucket extends AbstractLifeCycle implements ReadDataBucket {
	private static final int INTEGER_SIZE_BYTE = Integer.SIZE >> 3;

	private final File file;

	private final int bufSizeByte;

	private final int avgSizeByte;

	private DataInputStream input;

	private int position = 0;

	public SeqReadDataBucket(File file, int bufSizeByte, int avgSizeByte) {
		this.file = file;
		this.bufSizeByte = bufSizeByte;
		this.avgSizeByte = avgSizeByte;
	}

	@Override
	protected void doStart() {
		try {
			input = file2Stream(file);
			if (!input.markSupported()) {
				throw new UnsupportedOperationException("length read bucket should support mark.");
			}
		} catch (IOException io) {
			throw new IllegalStateException("failed to start read bucket.", io);
		}
	}

	@Override
	protected void doStop() {
		try {
			input.close();
		} catch (IOException ignore) {
		}
	}

	@Override
	public byte[] next() throws IOException {
		checkStop();

		try {
			input.mark(avgSizeByte);

			int len = input.readInt();

			if (len <= 0) {
				throw new IOException("failed to read next data. file: " + file.getName() + ", position: " + position);
			}

			byte[] data = new byte[len];
			input.readFully(data);
			position += (data.length + INTEGER_SIZE_BYTE);
			return data;
		} catch (IOException io) {
			try {
				input.reset();
			} catch (IOException ignore) {
			}

			throw io;
		}
	}

	@Override
	public void skip(long offset) throws IOException {
		checkStop();

		if (offset < 0) {
			throw new IllegalArgumentException("offset is negative");
		}

		long count = offset;
		while (count > 0) {
			long skipLength = input.skip(count);
			count -= skipLength;
		}
		position += offset;
	}

	@Override
	public int position() {
		return position;
	}

	protected DataInputStream file2Stream(File file) throws IOException {
		if (!file.canRead()) {
			throw new IOException("bucket can not read.");
		}

		if (GZipUtil.isGZip(file)) {
			input = new DataInputStream(new BufferedInputStream(
					new GZIPInputStream(new FileInputStream(file), bufSizeByte)));
		} else {
			input = new DataInputStream(new BufferedInputStream(new FileInputStream(file), bufSizeByte));
		}

		return input;
	}
}
