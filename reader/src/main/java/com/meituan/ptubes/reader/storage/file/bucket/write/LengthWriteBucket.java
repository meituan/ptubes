package com.meituan.ptubes.reader.storage.file.bucket.write;

import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public final class LengthWriteBucket extends AbstractLifeCycle implements WriteBucket {

	private static final int INTEGER_SIZE_BYTE = Integer.SIZE >> 3;

	private final File file;

	private final int bufSizeByte;

	private final int maxSizeByte;

	/**
	 * The offset in the file, generally pointing to the offset position of dataLength
	 * For multiple variable-length element storage
	 * The organization structure of the file is [int data1Length, byte[] data1, int data2Length, byte[] data2, xxx]
	 */
	private int offset;

	private DataOutputStream output;

	public LengthWriteBucket(File file, int bufSizeByte, int maxSizeByte) {
		this.file = file;
		this.bufSizeByte = bufSizeByte;
		this.maxSizeByte = maxSizeByte;
	}

	@Override
	protected void doStart() {
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			output = file2Stream(file);
		} catch (IOException io) {
			throw new RuntimeException("failed to start write bucket.", io);
		}
	}

	@Override
	protected void doStop() {
		try {
			output.flush();
		} catch (IOException ignore) {
		} finally {
			try {
				output.close();
			} catch (IOException ignore) {
			}
		}
	}

	@Override
	public void append(byte[] data) throws IOException {
		checkStop();

		output.writeInt(data.length);
		output.write(data);

		offset += (INTEGER_SIZE_BYTE + data.length);

	}

	@Override
	public void flush() throws IOException {
		checkStop();

		output.flush();
	}

	@Override
	public boolean hasRemainingForWrite(int needBytes) {
		checkStop();

		return offset + needBytes < maxSizeByte;
	}

	@Override
	public int position() {
		return offset;
	}

	protected DataOutputStream file2Stream(File file) throws IOException {
		if (!file.canWrite()) {
			throw new IOException("Bucket can not write. fileName: " + file.getName());
		}

		return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file, true), bufSizeByte));
	}
}
