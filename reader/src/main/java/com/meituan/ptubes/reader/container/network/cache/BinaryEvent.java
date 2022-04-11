package com.meituan.ptubes.reader.container.network.cache;

import com.meituan.ptubes.reader.container.network.encoder.EncoderType;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import java.util.concurrent.atomic.AtomicInteger;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;

public class BinaryEvent implements IEventCache {

	private final int partitionId;
	private final short partitionVal;
	private final BinlogInfo binlogInfo;
	private final EncoderType encoderType;
	private final Object encodedData;
	private final PtubesEvent oriData;
	private final AtomicInteger size; // use volatile instead

	public BinaryEvent(int partitionId, short partitionVal, BinlogInfo binlogInfo, EncoderType encoderType, Object data, PtubesEvent oriData) {
		this.partitionId = partitionId;
		this.partitionVal = partitionVal;
		this.binlogInfo = binlogInfo;
		this.encoderType = encoderType;
		this.encodedData = data;
		this.oriData = oriData;
		this.size = new AtomicInteger(-1);
	}

	public BinaryEvent(int partitionId, short partitionVal, BinlogInfo binlogInfo, EncoderType encoderType, Object data, PtubesEvent oriData, int size) {
		this.partitionId = partitionId;
		this.partitionVal = partitionVal;
		this.binlogInfo = binlogInfo;
		this.encoderType = encoderType;
		this.encodedData = data;
		this.oriData = oriData;
		this.size = new AtomicInteger(size);
	}

	public Object getEncodedData() {
		return encodedData;
	}

	public PtubesEvent getOriData() {
		return oriData;
	}

	public int getPartitionId() {
		return partitionId;
	}

	public BinlogInfo getBinlogInfo() {
		return binlogInfo;
	}

	public short getPartitionVal() {
		return partitionVal;
	}

	public EncoderType getEncoderType() {
		return encoderType;
	}

	@Override
	public int getSize() {
		if (this.size.get() != -1) {
			return this.size.get();
		} else {
			switch (this.encoderType) {
				case PROTOCOL_BUFFER:
					assert this.encodedData instanceof byte[];
					this.size.compareAndSet(-1, ((byte[]) this.encodedData).length);
					break;
				case RAW:
					assert this.encodedData instanceof byte[];
					this.size.compareAndSet(-1, ((byte[]) this.encodedData).length);
					break;
				default:
					break;
			}
		}
		return this.size.get();
	}

	// preserve
	public BinaryEvent shallowCopiedBinaryEvent(int partitionId) {
		return new BinaryEvent(partitionId, this.partitionVal, this.binlogInfo, this.encoderType, this.encodedData, this.oriData, this.size.get());
	}
}
