package com.meituan.ptubes.reader.storage.file.data.read;

import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.EventFactory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.bucket.BucketFactory;
import com.meituan.ptubes.reader.storage.file.bucket.read.ReadDataBucket;
import com.meituan.ptubes.reader.storage.file.data.DataManagerFinder;

public class SingleReadDataManager extends AbstractLifeCycle implements ReadDataManager<DataPosition, PtubesEvent> {
	private final File file;

	private final int bufSizeByte;

	private final int avgSizeByte;

	private ReadDataBucket readBucket;

	private DataPosition dataPosition;

	public SingleReadDataManager(File file, int bufSizeByte, int avgSizeByte) {
		this.file = file;
		this.bufSizeByte = bufSizeByte;
		this.avgSizeByte = avgSizeByte;
	}

	@Override
	protected void doStart() {
		readBucket = BucketFactory.newSeqReadDataBucket(file, bufSizeByte, avgSizeByte);
		readBucket.start();
	}

	@Override
	protected void doStop() {
		if (readBucket != null) {
			readBucket.stop();
		}
		DataManagerFinder.releaseDataFile(file.getAbsolutePath());
	}

	@Override
	public void open(DataPosition dataKey) throws IOException {
		checkStop();

		this.dataPosition = new DataPosition(dataKey.getCreationDate(), dataKey.getBucketNumber(), dataKey.getOffset());

		readBucket.skip(dataKey.getOffset());
	}

	@Override
	public PtubesEvent next() throws IOException {
		checkStop();

		byte[] data = readBucket.next();
		if (data == null) {
			throw new IOException("Read null data from data bucket");
		}
		return EventFactory.createReadOnlyEventFromBuffer(ByteBuffer.wrap(data), 0);
	}

	@Override
	public DataPosition position() {
		checkStop();
		return new DataPosition(dataPosition.getCreationDate(), dataPosition.getBucketNumber(),
				dataPosition.getOffset());
	}
}
