package com.meituan.ptubes.storage.utils;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.io.BinaryDecoder;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.vo.KeyPair;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;
import com.meituan.ptubes.reader.storage.common.event.MySQLChangeEntry;
import com.meituan.ptubes.sdk.protocol.RdsPacket;
import org.apache.commons.lang3.RandomUtils;


public class DbChangeEntryUtil {

	private static final Logger LOG = LoggerFactory.getLogger(DbChangeEntryUtil.class);

	private static final long localIp = 1234L;
	private static final ThreadLocal<BinaryDecoder> binDecoder = new ThreadLocal<BinaryDecoder>();

	public static ChangeEntry genRandomDbChangeEntry(MySQLBinlogInfo binlogInfo, String tableName) throws Exception {
		int index = RandomUtils.nextInt(0, Integer.MAX_VALUE) % 3;
		switch (index) {
		case 0:
			return genInsertDbChangeEntry(binlogInfo, tableName);
		case 1:
			return genUpdateDbChangeEntry(binlogInfo, tableName);
		case 2:
			return genDeleteDbChangeEntry(binlogInfo, tableName);
		default:
			return genInsertDbChangeEntry(binlogInfo, tableName);
		}
	}

	public static ChangeEntry genInsertDbChangeEntry(MySQLBinlogInfo binlogInfo, String tableName) throws Exception {
		VersionedSchema versionedSchema = TableUtil.getSchema(tableName);
		List<KeyPair> kps = new ArrayList<>();
		RdsPacket.RdsEvent insertEvent = TableUtil.genInsertPtubesRecord(versionedSchema, kps, binlogInfo);
		if (SourceType.MySQL.equals(binlogInfo.getSourceType())) {
			return new MySQLChangeEntry(tableName, localIp, (MySQLBinlogInfo) binlogInfo, System.currentTimeMillis(), System.nanoTime(), null,
				EventType.INSERT, kps, insertEvent);
		} else {
			throw new UnsupportedOperationException("Unsupport sourceType: " + binlogInfo.getSourceType());
		}
	}

	public static ChangeEntry genDeleteDbChangeEntry(MySQLBinlogInfo binlogInfo, String tableName) throws Exception {
		VersionedSchema versionedSchema = TableUtil.getSchema(tableName);
		List<KeyPair> kps = new ArrayList<>();
		RdsPacket.RdsEvent deleteEvent = TableUtil.genDeletePtubesRecord(versionedSchema, kps, binlogInfo);
		if (SourceType.MySQL.equals(binlogInfo.getSourceType())) {
			return new MySQLChangeEntry(tableName, localIp, (MySQLBinlogInfo) binlogInfo, System.currentTimeMillis(), System.nanoTime(), null,
				EventType.DELETE, kps, deleteEvent);
		} else {
			throw new UnsupportedOperationException("Unsupport sourceType: " + binlogInfo.getSourceType());
		}
	}

	public static ChangeEntry genUpdateDbChangeEntry(MySQLBinlogInfo binlogInfo, String tableName) throws Exception {
		VersionedSchema versionedSchema = TableUtil.getSchema(tableName);
		List<KeyPair> kps = new ArrayList<>();
		RdsPacket.RdsEvent updateEvent = TableUtil.genUpdatePtubesRecord(versionedSchema, kps, binlogInfo);
		if (SourceType.MySQL.equals(binlogInfo.getSourceType())) {
			return new MySQLChangeEntry(tableName, localIp, (MySQLBinlogInfo) binlogInfo, System.currentTimeMillis(), System.nanoTime(), null,
				EventType.UPDATE, kps, updateEvent);
		} else {
			throw new UnsupportedOperationException("Unsupport sourceType: " + binlogInfo.getSourceType());
		}
	}
}
