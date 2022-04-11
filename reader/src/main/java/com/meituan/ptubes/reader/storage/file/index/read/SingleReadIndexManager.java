package com.meituan.ptubes.reader.storage.file.index.read;

import com.meituan.ptubes.common.exception.GreaterThanStorageRangeException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.file.bucket.BucketFactory;
import com.meituan.ptubes.reader.storage.file.bucket.read.ReadIndexBucket;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;


public abstract class SingleReadIndexManager<K, V> extends AbstractLifeCycle implements ReadIndexManager<K, V> {
	protected final File file;

	private final int bufSizeByte;

	private final int avgSizeByte;

	/**
	 * The size of each index item, the size of key + value
	 * 	key: binlogInfo
	 * 	value: positionInfo
	 */
	private final int indexSizeByte;

	protected ReadIndexBucket readBucket;

	public SingleReadIndexManager(File file, int bufSizeByte, int avgSizeByte, int indexSizeByte) {
		this.file = file;
		this.bufSizeByte = bufSizeByte;
		this.avgSizeByte = avgSizeByte;
		this.indexSizeByte = indexSizeByte;
	}

	@Override
	protected void doStart() {
		readBucket = BucketFactory.newSeqReadIndexBucket(file, bufSizeByte, avgSizeByte, indexSizeByte);
		readBucket.start();
	}

	@Override
	protected void doStop() {
		if (readBucket != null) {
			readBucket.stop();
		}
	}

	public Pair<K, V> next() throws IOException {
		byte[] data = readBucket.next();
		return decode(data);
	}

	@Override
	public Pair<K, V> findOldest() throws IOException {
		checkStop();

		try {
			byte[] data = readBucket.next();
			return decode(data);
		} catch (EOFException eof) {
			return null;
		}
	}

	@Override
	public Pair<K, V> findLatest() throws IOException {
		checkStop();

		byte[] data = null;
		try {
			while (!isStopped()) {
				data = readBucket.next();
			}
			return null;
		} catch (EOFException eof) {
			if (data == null) {
				return null;
			}
			return decode(data);
		}
	}

	@Override
	public Pair<K, V> find(K indexKey)
			throws IOException, LessThanStorageRangeException, GreaterThanStorageRangeException {
		checkStop();

		byte[] data;
		Pair<K, V> oldPair = null;
		try {
			while (true) {
				data = readBucket.next();
				if (data != null) {
					Pair<K, V> pair = decode(data);
					// search for indexKey
					if (!greater(pair.getLeft(), indexKey)) {
						// indexKey >= pair
						oldPair = pair;
					} else {
						// find the first position greater than indexKey
						if (oldPair == null) {
							throw new LessThanStorageRangeException("failed to find binlog getKey : " + indexKey);
						} else {
							return oldPair;
						}
					}
				}
			}
		} catch (EOFException eof) {
			if (oldPair == null) {
				throw new GreaterThanStorageRangeException("failed to find key: " + indexKey);
			} else {
				return oldPair;
			}
		}
	}

	public ReadIndexBucket getReadBucket() {
		return readBucket;
	}

	public int position() {
		return readBucket.position();
	}

	protected abstract boolean greater(K aIndexKey, K bIndexKey);

	protected abstract Pair<K, V> decode(byte[] data) throws IOException;
}
