package com.meituan.ptubes.reader.storage.common;



import java.nio.ByteBuffer;

/**
 * <p>
 * The wrapper class for the DataPosition stored in the file
 * </p>
 * <p>
 * What is actually stored in the storage is the longValue<br> of the DataPosition
 * Composition of longValue:
 * <ol>
 * <li>High 32 digits represent date + hour, such as 12071717</li>
 * <li>The middle 32 digits represent the file number, such as 1,2,3</li>
 * <li>The lower 32 bits represent the offset of the file pointer</li>
 * </ol>
 * </p>
 * <p>
 * <b>It can be seen from the above, each file cannot be larger than 2G</b>
 * </p>
 */
public class DataPosition {
	private int creationDate;

	private int bucketNumber;

	private long offset;

	public static int getSizeInByte() {
		return 12;
	}

	public int getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(int creationDate) {
		this.creationDate = creationDate;
	}

	public int getBucketNumber() {
		return bucketNumber;
	}

	public void setBucketNumber(int bucketNumber) {
		this.bucketNumber = bucketNumber;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public DataPosition(DataPosition dataPosition) {
		this.creationDate = dataPosition.getCreationDate();
		this.bucketNumber = dataPosition.getBucketNumber();
		this.offset = dataPosition.getOffset();
	}

	public DataPosition(int creationDate, int bucketNumber, long offset) {
		this.creationDate = creationDate;
		this.bucketNumber = bucketNumber;
		this.offset = offset;
	}

	public DataPosition(String creationDate, int bucketNumber, long offset) {
		this.creationDate = Integer.valueOf(creationDate);
		this.bucketNumber = bucketNumber;
		this.offset = offset;
	}

	public byte[] encode() {
		ByteBuffer byteBuffer = ByteBuffer.allocate(getSizeInByte());
		byteBuffer.putInt(creationDate);
		byteBuffer.putInt(bucketNumber);
		byteBuffer.putInt((int) offset);
		return byteBuffer.array();
	}

	public static DataPosition decode(byte[] data) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(data);
		int creationDate = byteBuffer.getInt();
		int number = byteBuffer.getInt();
		long bucketOffset = Long.valueOf(byteBuffer.getInt());
		return new DataPosition(creationDate, number, bucketOffset);
	}

	@Override
	public String toString() {
		return "DataPosition{" + "creationDate=" + creationDate + ", bucketNumber=" + bucketNumber + ", offset="
				+ offset + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
            return true;
        }
		if (!(o instanceof DataPosition)) {
            return false;
        }

		DataPosition sequence = (DataPosition) o;

		return creationDate == sequence.creationDate &&
				bucketNumber == sequence.bucketNumber &&
				offset == sequence.offset;
	}

	@Override
	public int hashCode() {
		int result = creationDate;
		result = 31 * result + bucketNumber;
		result = (int) (31 * result + offset);
		return result;
	}
}

