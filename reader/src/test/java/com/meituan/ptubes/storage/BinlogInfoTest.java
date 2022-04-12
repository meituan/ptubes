package com.meituan.ptubes.storage;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;


public class BinlogInfoTest {
	private static final Logger LOG = LoggerFactory.getLogger(BinlogInfoTest.class);

	@Test
	public void testValid() {
		MySQLBinlogInfo binlogInfo = new MySQLBinlogInfo();
		Assert.assertFalse(binlogInfo.isValid());

		binlogInfo.setBinlogId(1);
		binlogInfo.setBinlogOffset(1);
		Assert.assertTrue(binlogInfo.isValid());

		binlogInfo = new MySQLBinlogInfo();
		binlogInfo.setTimestamp(1);
		Assert.assertTrue(binlogInfo.isValid());
	}

	@Test
	public void testCompaire() {
		short changeId = (short) 0;
		long serverId = RandomUtils.nextInt(0, Integer.MAX_VALUE);
		int binlogId = 10;
		long binlogOffset = 100L;
		byte[] uuid = RandomUtils.nextBytes(16);
		long txnId = RandomUtils.nextLong(0, Integer.MAX_VALUE);
		long eventIndex = 10L;
		long time = System.currentTimeMillis();

		{
			MySQLBinlogInfo aBinlogInfo = new MySQLBinlogInfo(changeId, serverId, binlogId, binlogOffset, uuid, txnId, eventIndex,
					time);
			MySQLBinlogInfo bBinlogInfo = new MySQLBinlogInfo(changeId, serverId, binlogId, binlogOffset, uuid, txnId, eventIndex,
					time);
			Assert.assertTrue(bBinlogInfo.isEqualTo(aBinlogInfo, StorageConstant.IndexPolicy.BINLOG_OFFSET));
		}

		{
			MySQLBinlogInfo aBinlogInfo = new MySQLBinlogInfo(changeId, serverId, binlogId, binlogOffset, uuid, txnId, eventIndex,
					time);
			MySQLBinlogInfo bBinlogInfo = new MySQLBinlogInfo((short) (changeId + 1), RandomUtils.nextInt(0, Integer.MAX_VALUE), binlogId - 1,
					binlogOffset + 1, uuid, txnId, eventIndex, time);
			Assert.assertTrue(bBinlogInfo.isGreaterThan(aBinlogInfo, StorageConstant.IndexPolicy.BINLOG_OFFSET));
		}

		{
			MySQLBinlogInfo aBinlogInfo = new MySQLBinlogInfo(changeId, serverId, binlogId, binlogOffset, uuid, txnId, eventIndex,
					time);
			MySQLBinlogInfo bBinlogInfo = new MySQLBinlogInfo(changeId, serverId, binlogId + 1, binlogOffset, uuid, txnId + 10,
					eventIndex, time);
			Assert.assertTrue(bBinlogInfo.isGreaterThan(aBinlogInfo, StorageConstant.IndexPolicy.BINLOG_OFFSET));
		}

		{
			MySQLBinlogInfo aBinlogInfo = new MySQLBinlogInfo(changeId, serverId, binlogId, binlogOffset, uuid, txnId, eventIndex,
					time);
			MySQLBinlogInfo bBinlogInfo = new MySQLBinlogInfo(changeId, serverId, binlogId, binlogOffset + 10, uuid, txnId + 10,
					eventIndex, time);
			Assert.assertTrue(bBinlogInfo.isGreaterThan(aBinlogInfo, StorageConstant.IndexPolicy.BINLOG_OFFSET));
		}

		{
			MySQLBinlogInfo aBinlogInfo = new MySQLBinlogInfo(changeId, serverId, binlogId, binlogOffset, uuid, txnId, eventIndex,
					time);
			MySQLBinlogInfo bBinlogInfo = new MySQLBinlogInfo(changeId, serverId, binlogId, binlogOffset, uuid, txnId,
					eventIndex + 10, time);
			Assert.assertTrue(bBinlogInfo.isGreaterThan(aBinlogInfo, StorageConstant.IndexPolicy.BINLOG_OFFSET));
		}
		LOG.info("test finished");
	}

}
