package com.meituan.ptubes.reader.container.common.constants;


public class StorageConstant {
	public static final int KB = 1024;
	public static final int MB = 1024 * KB;
	public static final int GB = 1024 * MB;
	public static final int MINUT = 60;
	public static final int DAY = 60 * MINUT;
	public static final String CAT_STORAGE_LATENCY_TYPE = "Ptubes.StorageEngine.LatencyEventRecieve";
	public static final String CAT_STORAGE_WRITE_TYPE = "Ptubes.StorageEngine.WriteEvent"; // Event placement time
	public static final String CAT_STORAGE_READ_TYPE = "Ptubes.StorageEngine.ReadEvent";

	public static final String COMPOUND_KEY_DELIMITER = "\t";

	public enum StorageMode {
		MEM, FILE, MIX
	}

	public enum IndexPolicy {
		BINLOG_OFFSET, TIME
	}

	public enum StorageStatus {
		NORMAL, CORRUPTION, READONLY
	}

	public enum StorageRangeCheckResult {
		//In the range of stored data, it is less than the minimum binloginfo of the storage layer and greater than the maximum binloginfo of the storage layer
		IN_RANGE, LESS_THAN_MIN, GREATER_THAN_MAX, INVALID
	}
}
