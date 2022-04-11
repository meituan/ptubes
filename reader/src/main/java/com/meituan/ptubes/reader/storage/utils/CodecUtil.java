package com.meituan.ptubes.reader.storage.utils;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.mem.buffer.BinlogInfoFactory;
import org.apache.commons.lang3.tuple.Pair;


/**
 * Put it into StorageFactory to initialize together
 * CodecUtilForMySQLBinlogInfo
 * If there are other third-party data sources, just add a class, and StorageFactory can create a series of corresponding service components according to SourceType
 */
public class CodecUtil {
	public static byte[] encodeL1Index(BinlogInfo binlogInfo, DataPosition dataPosition) {
		return encodeL2Index(binlogInfo, dataPosition);
	}


	public static byte[] encodeL2Index(BinlogInfo binlogInfo, DataPosition dataPosition) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(binlogInfo.getLength() + DataPosition.getSizeInByte());
		byteBuffer.put(binlogInfo.encode());
		byteBuffer.put(dataPosition.encode());

		return byteBuffer.array();
	}

	public static Pair<BinlogInfo, DataPosition> decodeL1Index(byte[] data, SourceType sourceType) {
		if (SourceType.MySQL.equals(sourceType)) {
			MySQLBinlogInfo binlogInfo = (MySQLBinlogInfo) BinlogInfoFactory.decode(SourceType.MySQL, Arrays.copyOfRange(data, 0, MySQLBinlogInfo.LENGTH));
			DataPosition dataPosition = DataPosition.decode(Arrays.copyOfRange(data, binlogInfo.getLength(), data.length));
			return Pair.of(binlogInfo, dataPosition);
		} else {
			throw new UnsupportedOperationException("Unsupported sourceType: " + sourceType);
		}
	}

	public static Pair<BinlogInfo, DataPosition> decodeL2Index(byte[] data, SourceType sourceType) {
		return decodeL1Index(data, sourceType);
	}

}
