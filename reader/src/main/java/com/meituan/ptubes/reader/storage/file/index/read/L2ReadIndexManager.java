package com.meituan.ptubes.reader.storage.file.index.read;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.index.SeriesIndexManagerFinder;
import com.meituan.ptubes.reader.storage.utils.CodecUtil;
import java.io.File;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import org.apache.commons.lang3.tuple.Pair;


public final class L2ReadIndexManager extends SingleReadIndexManager<BinlogInfo, DataPosition> {
	private final StorageConstant.IndexPolicy indexPolicy;
	private final SourceType sourceType;

	public L2ReadIndexManager(File file, int bufSizeByte, int avgSizeByte, StorageConstant.IndexPolicy indexPolicy,
		SourceType sourceType) {
		super(file, bufSizeByte, avgSizeByte, MySQLBinlogInfo.getSizeInByte() + DataPosition.getSizeInByte());
		this.indexPolicy = indexPolicy;
		this.sourceType = sourceType;
	}

	@Override
	protected void doStop() {
		super.doStop();
		SeriesIndexManagerFinder.releaseIndexFile(this.file.getAbsolutePath());
	}

	/**
	 *
	 * @param aIndexKey event from storage
	 * @param bIndexKey search
	 * @return
	 */
	@Override
	protected boolean greater(BinlogInfo aIndexKey, BinlogInfo bIndexKey) {
		switch (sourceType) {
			case MySQL:
				return aIndexKey.isGreaterThan(bIndexKey, indexPolicy);
			default:
				throw new PtubesRunTimeException("Unsupported binlog info comparison in source type " + sourceType.name());
		}
	}

	@Override
	protected Pair<BinlogInfo, DataPosition> decode(byte[] data) throws IOException {
		return CodecUtil.decodeL2Index(data, sourceType);
	}
}
