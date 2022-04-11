package com.meituan.ptubes.reader.storage.file.index.write;

import com.meituan.ptubes.reader.storage.utils.CodecUtil;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.index.SeriesIndexManagerFinder;

public class L2WriteIndexManager extends SingleWriteIndexManager<BinlogInfo, DataPosition> {

	public L2WriteIndexManager(File file, String date, int number, int bufSizeByte, int maxSizeByte) {
		super(file, date, number, bufSizeByte, maxSizeByte);
	}

	@Override
	protected void doStop() {
		super.doStop();
		SeriesIndexManagerFinder.releaseIndexFile(this.file.getAbsolutePath());
	}

	@Override
	public void append(BinlogInfo indexKey, DataPosition indexValue) throws IOException {
		super.append(indexKey, indexValue);
	}

	@Override
	public void clean(Date date) throws IOException {
		
	}

	@Override
	protected byte[] encode(BinlogInfo indexKey, DataPosition indexValue) {
		return CodecUtil.encodeL1Index(indexKey, indexValue);
	}
}
