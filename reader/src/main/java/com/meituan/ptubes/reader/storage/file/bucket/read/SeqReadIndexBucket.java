package com.meituan.ptubes.reader.storage.file.bucket.read;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.GZipUtil;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import com.meituan.ptubes.reader.storage.file.bucket.write.LengthWriteBucket;

public final class SeqReadIndexBucket extends AbstractLifeCycle implements ReadIndexBucket {
	private final static Logger LOG = LoggerFactory.getLogger(SeqReadIndexBucket.class);
	private static final int INTEGER_SIZE_BYTE = Integer.SIZE >> 3;

	private final File file;

	private final int bufSizeByte;

	private final int avgSizeByte;

	private final int indexSizeByte;

	private DataInputStream input;

	private int position = 0;

	public SeqReadIndexBucket(File file, int bufSizeByte, int avgSizeByte, int indexSizeByte) {
		this.file = file;
		this.bufSizeByte = bufSizeByte;
		this.avgSizeByte = avgSizeByte;
		this.indexSizeByte = indexSizeByte;
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
			LOG.warn("Input close error", ignore.getMessage());
		}
	}

	/**
	 * {@link LengthWriteBucket#offset}
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] next() throws IOException {
		checkStop();

		try {
			input.mark(avgSizeByte);

			int len = input.readInt();

			if (len <= 0) {
				throw new EOFException("Reach the end of line reader bucket. " + file.getName());
			}

			byte[] data = new byte[len];
			input.readFully(data);
			position += (data.length + INTEGER_SIZE_BYTE);
			return data;
		} catch (IOException io) {
			try {
				input.reset();
			} catch (IOException ignore) {
				LOG.warn("Input reset error", ignore.getMessage());
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
