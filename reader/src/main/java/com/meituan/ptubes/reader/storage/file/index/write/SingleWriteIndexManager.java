package com.meituan.ptubes.reader.storage.file.index.write;

import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.bucket.BucketFactory;
import com.meituan.ptubes.reader.storage.file.bucket.write.LengthWriteBucket;
import java.io.File;
import java.io.IOException;

public abstract class SingleWriteIndexManager<K, V> extends AbstractLifeCycle implements WriteIndexManager<K, V> {
	protected final File file;

	private final int bufSizeByte;

	private final int maxSizeByte;

	private String date;

	private int number;

	private LengthWriteBucket writeBucket;

	// l1 index
	public SingleWriteIndexManager(File file, int bufSizeByte, int maxSizeByte) {
		this.file = file;
		this.bufSizeByte = bufSizeByte;
		this.maxSizeByte = maxSizeByte;
	}

	// l2 index
	public SingleWriteIndexManager(File file, String date, int number, int bufSizeByte, int maxSizeByte) {
		this.file = file;
		this.date = date;
		this.number = number;
		this.bufSizeByte = bufSizeByte;
		this.maxSizeByte = maxSizeByte;
	}

	@Override
	protected void doStart() {
		writeBucket = BucketFactory.newLengthWriteBucket(file, bufSizeByte, maxSizeByte);
		writeBucket.start();
	}

	@Override
	protected void doStop() {
		if (writeBucket != null) {
			writeBucket.stop();
		}
	}

	@Override
	public void append(K indexKey, V indexValue) throws IOException {
		checkStop();

		writeBucket.append(encode(indexKey, indexValue));
	}

	@Override
	public void flush() throws IOException {
		checkStop();

		if (writeBucket != null) {
			writeBucket.flush();
		}
	}

	@Override
	public DataPosition position() {
		checkStop();

		return new DataPosition(date, number, writeBucket.position());
	}

	protected boolean hasRemainingForWrite(int needBytes) {
		checkStop();

		return writeBucket != null && writeBucket.hasRemainingForWrite(needBytes);
	}

	abstract protected byte[] encode(K indexKey, V indexValue);
}
