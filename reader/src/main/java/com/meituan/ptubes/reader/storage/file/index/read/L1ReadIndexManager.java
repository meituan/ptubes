package com.meituan.ptubes.reader.storage.file.index.read;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.utils.CodecUtil;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;


public final class L1ReadIndexManager extends SingleReadIndexManager<BinlogInfo, DataPosition> {
	private final StorageConstant.IndexPolicy indexPolicy;
	private final SourceType sourceType;

	public L1ReadIndexManager(File file, int bufSizeByte, int avgSizeByte, StorageConstant.IndexPolicy indexPolicy,
		SourceType sourceType) {
		super(file, bufSizeByte, avgSizeByte, MySQLBinlogInfo.getSizeInByte() + DataPosition.getSizeInByte());
		this.indexPolicy = indexPolicy;
		this.sourceType = sourceType;
	}

	@Override
	public Pair<BinlogInfo, DataPosition> next() throws IOException {
		try {
			return decode(readBucket.next());
		} catch (EOFException eof) {
			return null;
		}
	}

	/**
	 *
	 * @param aIndexKey binlogInfo of data from storage
	 * @param bIndexKey treat as searchBinlogInfo
	 * @return
	 */
	
	@Override
	public boolean greater(BinlogInfo aIndexKey, BinlogInfo bIndexKey) {
		switch (sourceType) {
			case MySQL:
				return aIndexKey.isGreaterThan(bIndexKey, indexPolicy);
			default:
				throw new PtubesRunTimeException("Unsupported binlog info comparison in source type " + sourceType.name());
		}
	}

	@Override
	protected Pair<BinlogInfo, DataPosition> decode(byte[] data) {
		return CodecUtil.decodeL1Index(data, sourceType);
	}
}
