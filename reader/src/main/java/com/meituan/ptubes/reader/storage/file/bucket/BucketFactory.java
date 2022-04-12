package com.meituan.ptubes.reader.storage.file.bucket;

import com.meituan.ptubes.reader.storage.file.bucket.read.SeqReadDataBucket;
import com.meituan.ptubes.reader.storage.file.bucket.read.SeqReadIndexBucket;
import java.io.File;
import com.meituan.ptubes.reader.storage.file.bucket.write.LengthWriteBucket;

public final class BucketFactory {

	private BucketFactory() {
	}

	public static LengthWriteBucket newLengthWriteBucket(File file, int bufSizeByte, int maxSizeByte) {
		return new LengthWriteBucket(file, bufSizeByte, maxSizeByte);
	}

	public static SeqReadDataBucket newSeqReadDataBucket(File file, int bufSizeByte, int avgSizeByte) {
		return new SeqReadDataBucket(file, bufSizeByte, avgSizeByte);
	}

	public static SeqReadIndexBucket newSeqReadIndexBucket(File file, int bufSizeByte, int avgSizeByte, int indexSizeByte) {
		return new SeqReadIndexBucket(file, bufSizeByte, avgSizeByte, indexSizeByte);
	}
}
