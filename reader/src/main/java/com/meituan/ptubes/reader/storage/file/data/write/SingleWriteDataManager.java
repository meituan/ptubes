package com.meituan.ptubes.reader.storage.file.data.write;

import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.file.data.DataManagerFinder;
import java.io.File;
import java.io.IOException;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.bucket.BucketFactory;
import com.meituan.ptubes.reader.storage.file.bucket.write.LengthWriteBucket;

/**
 * Minimum granularity (hours) for file writing to manager
 */
public class SingleWriteDataManager extends AbstractLifeCycle implements WriteDataManager<DataPosition, byte[]> {
	/**
	 * bucket-*.data files
	 */
	private final File file;

	/**
	 * File IO buffer size
	 * Default 0.2M(from lion)
	 */
	private final int bufSizeByte;

	/**
	 * single file size
	 * Default 1G(from lion)
	 */
	private final int maxSizeByte;

	/**
	 * date-hour string
	 * eg. 2021012115
	 */
	private final String date;

	/**
	 * bucket number (within an hour)
	 */
	private final int number;

	private LengthWriteBucket writeBucket;

	public SingleWriteDataManager(File file, String date, int number, StorageConfig.FileConfig fileConfig) {
		this.file = file;
		this.date = date;
		this.number = number;
		this.bufSizeByte = fileConfig.getDataWriteBufSizeInByte();
		this.maxSizeByte = fileConfig.getDataBucketSizeInByte();
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
		DataManagerFinder.releaseDataFile(file.getAbsolutePath());
	}

	@Override
	public DataPosition append(byte[] dataValue) throws IOException {
		checkStop();

		DataPosition position = position();

		writeBucket.append(dataValue);
		if (position.getOffset() == 0) {
			// The first data flush, to avoid the need to delete files due to data corruption
			writeBucket.flush();
		}

		return position;
	}

	@Override
	public void flush() throws IOException {
		checkStop();

		writeBucket.flush();
	}

	protected boolean hasRemainingForWrite(int needBytes) {
		checkStop();

		return writeBucket.hasRemainingForWrite(needBytes);
	}

	@Override
	public DataPosition position() {
		checkStop();

		return new DataPosition(date, number, writeBucket.position());
	}

}
