package com.meituan.ptubes.storage;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.storage.utils.CodecUtil;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;


public class CodecUtilTest {

	@Test
	public void testMySQLEncodeDecode() throws Exception {
		MySQLBinlogInfo binlogInfo = new MySQLBinlogInfo((short) 0, RandomUtils.nextInt(0, Integer.MAX_VALUE), 1, 10L, RandomUtils.nextBytes(16), 10L,
				0, System.currentTimeMillis());
		DataPosition dataPosition = new DataPosition(20190805, 0, 100);
		byte[] data = CodecUtil.encodeL1Index(binlogInfo, dataPosition);
		Pair<BinlogInfo, DataPosition> pair = CodecUtil.decodeL1Index(data, SourceType.MySQL);
		Assert.assertTrue(binlogInfo.isEqualTo(pair.getLeft(), StorageConstant.IndexPolicy.BINLOG_OFFSET));
		Assert.assertEquals(dataPosition, pair.getRight());
	}
}
