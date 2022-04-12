package com.meituan.ptubes.reader.container.common.config.storage;

import com.meituan.ptubes.reader.container.common.constants.AssertLevel;
import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;

public class StorageConfig {
	public static final int DEFAULT_MAX_EVENT_SIZE = -1;
	public static final double DEFAULT_DEFAULT_MEMUSAGE = 0.75;
	public static final double DEFAULT_EVENT_BUFFER_MAX_SIZE_QUOTA = 0.8;
	public static final double DEFAULT_EVENT_BUFFER_READ_BUFFER_QUOTA = 0.1;
	public static final EventBufferConstants.QueuePolicy DEFAULT_QUEUE_POLICY = EventBufferConstants.QueuePolicy.OVERWRITE_ON_WRITE;
	// Maximum individual Buffer Size
	private static int FIVE_HUNDRED_MEGABYTES_IN_BYTES = 500 * StorageConstant.MB;
	public static final int DEFAULT_INDIVIDUAL_BUFFER_SIZE = FIVE_HUNDRED_MEGABYTES_IN_BYTES;
	public static final String DEFAULT_MMAP_DIRECTORY = "mmappedBuffer";
	public static final long BUFFER_REMOVE_WAIT_PERIOD = 3600 * 24;
	public static final int DEFAULT_AVERAGE_EVENT_SIZE = 20 * 1024;

	private String readerTaskName;
	private volatile StorageConstant.StorageMode storageMode;
	// Data over time will be cleaned up
	private volatile int fileRetentionHours = 24;
	// Data files older than time will be compressed
	private volatile int fileCompressHours = -1;
	// The maximum time to delay the execution of expireTask after the task is started
	private int maxExpireTaskDelayMinutes = 10;
	private StorageConstant.IndexPolicy indexPolicy = StorageConstant.IndexPolicy.BINLOG_OFFSET;
	private MemConfig memConfig;
	private FileConfig fileConfig;

	public StorageConfig() {

	}

	public StorageConfig(StorageConstant.StorageMode storageMode) {
		this.storageMode = storageMode;
	}

	public static class MemConfig {
		private int readBufferSizeInByte = 10240;
		private int maxIndexSizeInByte = 102400;
		private long maxSizeInByte = 214748364; // Buffer total size
		private int maxIndividualBufferSizeInByte = 524288000; // ByteBuffer size per block
		private int maxEventSizeInByte = -1;
		private int averageEventSizeInByte = DEFAULT_AVERAGE_EVENT_SIZE;
		protected EventBufferConstants.AllocationPolicy allocationPolicy = EventBufferConstants.AllocationPolicy.DIRECT_MEMORY;
		protected double defaultMemUsage = 0.75;
		protected EventBufferConstants.QueuePolicy queuePolicy = null;
		private AssertLevel assertLevel = AssertLevel.NONE;
		private long bufferRemoveWaitPeriodSec = 86400;
		private boolean restoreMMappedBuffers = false;
		private boolean restoreMMappedBuffersValidateEvents = false;
		private boolean enableIndex = true;

		public MemConfig() {
		}

		public MemConfig(long memBufferSize) {
			this.maxSizeInByte = memBufferSize;
		}

		public int getReadBufferSizeInByte() {
			return readBufferSizeInByte;
		}

		public void setReadBufferSizeInByte(int readBufferSizeInByte) {
			this.readBufferSizeInByte = readBufferSizeInByte;
		}

		public int getMaxIndexSizeInByte() {
			return maxIndexSizeInByte;
		}

		public void setMaxIndexSizeInByte(int maxIndexSizeInByte) {
			this.maxIndexSizeInByte = maxIndexSizeInByte;
		}

		public long getMaxSizeInByte() {
			return maxSizeInByte;
		}

		public void setMaxSizeInByte(long maxSizeInByte) {
			this.maxSizeInByte = maxSizeInByte;
		}

		public int getMaxIndividualBufferSizeInByte() {
			return maxIndividualBufferSizeInByte;
		}

		public void setMaxIndividualBufferSizeInByte(int maxIndividualBufferSizeInByte) {
			this.maxIndividualBufferSizeInByte = maxIndividualBufferSizeInByte;
		}

		public int getMaxEventSizeInByte() {
			return maxEventSizeInByte;
		}

		public void setMaxEventSizeInByte(int maxEventSizeInByte) {
			this.maxEventSizeInByte = maxEventSizeInByte;
		}

		public int getAverageEventSizeInByte() {
			return averageEventSizeInByte;
		}

		public void setAverageEventSizeInByte(int averageEventSizeInByte) {
			this.averageEventSizeInByte = averageEventSizeInByte;
		}

		public EventBufferConstants.AllocationPolicy getAllocationPolicy() {
			return allocationPolicy;
		}

		public void setAllocationPolicy(EventBufferConstants.AllocationPolicy allocationPolicy) {
			this.allocationPolicy = allocationPolicy;
		}

		public double getDefaultMemUsage() {
			return defaultMemUsage;
		}

		public void setDefaultMemUsage(double defaultMemUsage) {
			this.defaultMemUsage = defaultMemUsage;
		}

		public EventBufferConstants.QueuePolicy getQueuePolicy() {
			return queuePolicy;
		}

		public void setQueuePolicy(EventBufferConstants.QueuePolicy queuePolicy) {
			this.queuePolicy = queuePolicy;
		}

		public AssertLevel getAssertLevel() {
			return assertLevel;
		}

		public void setAssertLevel(AssertLevel assertLevel) {
			this.assertLevel = assertLevel;
		}

		public long getBufferRemoveWaitPeriodSec() {
			return bufferRemoveWaitPeriodSec;
		}

		public void setBufferRemoveWaitPeriodSec(long bufferRemoveWaitPeriodSec) {
			this.bufferRemoveWaitPeriodSec = bufferRemoveWaitPeriodSec;
		}

		public boolean isRestoreMMappedBuffers() {
			return restoreMMappedBuffers;
		}

		public void setRestoreMMappedBuffers(boolean restoreMMappedBuffers) {
			this.restoreMMappedBuffers = restoreMMappedBuffers;
		}

		public boolean isRestoreMMappedBuffersValidateEvents() {
			return restoreMMappedBuffersValidateEvents;
		}

		public void setRestoreMMappedBuffersValidateEvents(boolean restoreMMappedBuffersValidateEvents) {
			this.restoreMMappedBuffersValidateEvents = restoreMMappedBuffersValidateEvents;
		}

		public boolean isEnableIndex() {
			return enableIndex;
		}

		public void setEnableIndex(boolean enableIndex) {
			this.enableIndex = enableIndex;
		}

		@Override
		public String toString() {
			return "MemConfig{" + "readBufferSizeInByte=" + readBufferSizeInByte + ", maxIndexSizeInByte="
					+ maxIndexSizeInByte + ", maxSizeInByte=" + maxSizeInByte + ", maxIndividualBufferSizeInByte="
					+ maxIndividualBufferSizeInByte + ", maxEventSizeInByte=" + maxEventSizeInByte
					+ ", averageEventSizeInByte=" + averageEventSizeInByte + ", allocationPolicy=" + allocationPolicy
					+ ", defaultMemUsage=" + defaultMemUsage + ", queuePolicy=" + queuePolicy + ", assertLevel="
					+ assertLevel + ", bufferRemoveWaitPeriodSec=" + bufferRemoveWaitPeriodSec
					+ ", restoreMMappedBuffers=" + restoreMMappedBuffers + ", restoreMMappedBuffersValidateEvents="
					+ restoreMMappedBuffersValidateEvents + ", enableIndex=" + enableIndex + '}';
		}
	}

	public static class FileConfig {
		private int dataWriteBufSizeInByte = 2 * StorageConstant.MB;
		private int dataReadBufSizeInByte = 2 * StorageConstant.MB;
		private int dataReadAvgSizeInByte = 10 * StorageConstant.KB;
		private int dataBucketSizeInByte = 1 * StorageConstant.GB;
		private int blockSizeInByte = 64 * StorageConstant.KB; // Block size, index sparsity. how many data ranges an index
		private int l1WriteBufSizeInByte = 2 * StorageConstant.MB;
		private int l1ReadBufSizeInByte = 2 * StorageConstant.MB;
		private int l1ReadAvgSizeInByte = 1 * StorageConstant.KB;
		private int l1BucketSizeInByte = 1 * StorageConstant.GB;
		private int l2WriteBufSizeInByte = 2 * StorageConstant.MB;
		private int l2ReadBufSizeInByte = 2 * StorageConstant.MB;
		private int l2ReadAvgSizeInByte = 1 * StorageConstant.KB;
		private int l2BucketSizeInByte = 1 * StorageConstant.GB;

		public FileConfig() {

		}

		public FileConfig(int dataWriteBufSizeInByte, int dataReadBufSizeInByte, int dataReadAvgSizeInByte,
				int dataBucketSizeInByte, int blockSizeInByte, int l1WriteBufSizeInByte, int l1ReadBufSizeInByte,
				int l1ReadAvgSizeInByte, int l1BucketSizeInByte, int l2WriteBufSizeInByte, int l2ReadBufSizeInByte,
				int l2ReadAvgSizeInByte, int l2BucketSizeInByte) {
			this.dataWriteBufSizeInByte = dataWriteBufSizeInByte;
			this.dataReadBufSizeInByte = dataReadBufSizeInByte;
			this.dataReadAvgSizeInByte = dataReadAvgSizeInByte;
			this.dataBucketSizeInByte = dataBucketSizeInByte;
			this.blockSizeInByte = blockSizeInByte;
			this.l1WriteBufSizeInByte = l1WriteBufSizeInByte;
			this.l1ReadBufSizeInByte = l1ReadBufSizeInByte;
			this.l1ReadAvgSizeInByte = l1ReadAvgSizeInByte;
			this.l1BucketSizeInByte = l1BucketSizeInByte;
			this.l2WriteBufSizeInByte = l2WriteBufSizeInByte;
			this.l2ReadBufSizeInByte = l2ReadBufSizeInByte;
			this.l2ReadAvgSizeInByte = l2ReadAvgSizeInByte;
			this.l2BucketSizeInByte = l2BucketSizeInByte;
		}

		public int getDataWriteBufSizeInByte() {
			return dataWriteBufSizeInByte;
		}

		public void setDataWriteBufSizeInByte(int dataWriteBufSizeInByte) {
			this.dataWriteBufSizeInByte = dataWriteBufSizeInByte;
		}

		public int getDataReadBufSizeInByte() {
			return dataReadBufSizeInByte;
		}

		public void setDataReadBufSizeInByte(int dataReadBufSizeInByte) {
			this.dataReadBufSizeInByte = dataReadBufSizeInByte;
		}

		public int getDataReadAvgSizeInByte() {
			return dataReadAvgSizeInByte;
		}

		public void setDataReadAvgSizeInByte(int dataReadAvgSizeInByte) {
			this.dataReadAvgSizeInByte = dataReadAvgSizeInByte;
		}

		public int getDataBucketSizeInByte() {
			return dataBucketSizeInByte;
		}

		public void setDataBucketSizeInByte(int dataBucketSizeInByte) {
			this.dataBucketSizeInByte = dataBucketSizeInByte;
		}

		public int getBlockSizeInByte() {
			return blockSizeInByte;
		}

		public void setBlockSizeInByte(int blockSizeInByte) {
			this.blockSizeInByte = blockSizeInByte;
		}

		public int getL1WriteBufSizeInByte() {
			return l1WriteBufSizeInByte;
		}

		public void setL1WriteBufSizeInByte(int l1WriteBufSizeInByte) {
			this.l1WriteBufSizeInByte = l1WriteBufSizeInByte;
		}

		public int getL1ReadBufSizeInByte() {
			return l1ReadBufSizeInByte;
		}

		public void setL1ReadBufSizeInByte(int l1ReadBufSizeInByte) {
			this.l1ReadBufSizeInByte = l1ReadBufSizeInByte;
		}

		public int getL1ReadAvgSizeInByte() {
			return l1ReadAvgSizeInByte;
		}

		public void setL1ReadAvgSizeInByte(int l1ReadAvgSizeInByte) {
			this.l1ReadAvgSizeInByte = l1ReadAvgSizeInByte;
		}

		public int getL1BucketSizeInByte() {
			return l1BucketSizeInByte;
		}

		public void setL1BucketSizeInByte(int l1BucketSizeInByte) {
			this.l1BucketSizeInByte = l1BucketSizeInByte;
		}

		public int getL2WriteBufSizeInByte() {
			return l2WriteBufSizeInByte;
		}

		public void setL2WriteBufSizeInByte(int l2WriteBufSizeInByte) {
			this.l2WriteBufSizeInByte = l2WriteBufSizeInByte;
		}

		public int getL2ReadBufSizeInByte() {
			return l2ReadBufSizeInByte;
		}

		public void setL2ReadBufSizeInByte(int l2ReadBufSizeInByte) {
			this.l2ReadBufSizeInByte = l2ReadBufSizeInByte;
		}

		public int getL2ReadAvgSizeInByte() {
			return l2ReadAvgSizeInByte;
		}

		public void setL2ReadAvgSizeInByte(int l2ReadAvgSizeInByte) {
			this.l2ReadAvgSizeInByte = l2ReadAvgSizeInByte;
		}

		public int getL2BucketSizeInByte() {
			return l2BucketSizeInByte;
		}

		public void setL2BucketSizeInByte(int l2BucketSizeInByte) {
			this.l2BucketSizeInByte = l2BucketSizeInByte;
		}
	}

	public String getReaderTaskName() {
		return readerTaskName;
	}

	public void setReaderTaskName(String readerTaskName) {
		this.readerTaskName = readerTaskName;
	}

	public StorageConstant.StorageMode getStorageMode() {
		return storageMode;
	}

	public void setStorageMode(StorageConstant.StorageMode storageMode) {
		this.storageMode = storageMode;
	}

	public int getFileRetentionHours() {
		return fileRetentionHours;
	}

	public void setFileRetentionHours(int fileRetentionHours) {
		this.fileRetentionHours = fileRetentionHours;
	}

	public int getFileCompressHours() {
		return fileCompressHours;
	}

	public void setFileCompressHours(int fileCompressHours) {
		this.fileCompressHours = fileCompressHours;
	}

	public int getMaxExpireTaskDelayMinutes() {
		return maxExpireTaskDelayMinutes;
	}

	public void setMaxExpireTaskDelayMinutes(int maxExpireTaskDelayMinutes) {
		this.maxExpireTaskDelayMinutes = maxExpireTaskDelayMinutes;
	}

	public StorageConstant.IndexPolicy getIndexPolicy() {
		return indexPolicy;
	}

	public void setIndexPolicy(StorageConstant.IndexPolicy indexPolicy) {
		this.indexPolicy = indexPolicy;
	}

	public MemConfig getMemConfig() {
		return memConfig;
	}

	public void setMemConfig(MemConfig memConfig) {
		this.memConfig = memConfig;
	}

	public FileConfig getFileConfig() {
		return fileConfig;
	}

	public void setFileConfig(FileConfig fileConfig) {
		this.fileConfig = fileConfig;
	}
}
