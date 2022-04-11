package com.meituan.ptubes.reader.storage.common.event;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import java.util.ArrayList;
import java.util.List;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.PtubesFieldType;
import com.meituan.ptubes.reader.container.common.vo.KeyPair;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;


public final class ChangeEntryFactory {
	public static final String SENTINEL_ENTRY_NAME = "__buffaloDB__.__buffaloTableSentinel_";
	public static final String COMMIT_ENTRY_NAME = "__buffaloDB__.__buffaloTableCommit_";

	public static ChangeEntry createSentinelEntry(BinlogInfo binlogInfo) {
		List<KeyPair> partitionKeys = new ArrayList<>();
		KeyPair keyPair = new KeyPair(-1L, PtubesFieldType.LONG);
		partitionKeys.add(keyPair);
		return new MySQLChangeEntry(SENTINEL_ENTRY_NAME, -1, (MySQLBinlogInfo) binlogInfo, binlogInfo.getTs(),
			System.nanoTime(), null, EventType.SENTINEL, partitionKeys, null);
	}

	public static ChangeEntry createCommitEntry(BinlogInfo binlogInfo) {
		if (SourceType.MySQL.equals(binlogInfo.getSourceType())) {
			List<KeyPair> partitionKeys = new ArrayList<>();
			KeyPair keyPair = new KeyPair(-1L, PtubesFieldType.LONG);
			partitionKeys.add(keyPair);
			return new MySQLChangeEntry(COMMIT_ENTRY_NAME, -1, (MySQLBinlogInfo) binlogInfo, binlogInfo.getTs(),
					System.nanoTime(), null, EventType.COMMIT, partitionKeys, null);
		} else {
			throw new UnsupportedOperationException("Unsupported sourceType: " + binlogInfo.getSourceType());
		}
	}
}
