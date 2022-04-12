package com.meituan.ptubes.reader.container.network.encoder;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.network.cache.BinaryEvent;
import com.meituan.ptubes.sdk.protocol.RdsPacket;

public class ProtocalBufferEncoder implements IEventToByteEncoder {

	public static final ProtocalBufferEncoder INSTANCE = new ProtocalBufferEncoder();

	@Override
	public byte[] encodePackage(SourceType sourceType, List<BinaryEvent> binaryEvents) {
		RdsPacket.RdsEnvelope.Builder envelopeBuilder = RdsPacket.RdsEnvelope.newBuilder();
		envelopeBuilder.setTotalEventCount(binaryEvents.size())
			.setMessageType(RdsPacket.MessageType.PARTITIONEDMESSAGE);
		if (binaryEvents.size() == 0) {
			// done
		} else {
			RdsPacket.RdsPartitionedMessage.Builder partMsgBuilder = RdsPacket.RdsPartitionedMessage.newBuilder();
			Map<Integer, List<BinaryEvent>> partEvents = new HashMap<>();
			for (BinaryEvent binaryEvent : binaryEvents) {
				List<BinaryEvent> onePartEvents = partEvents.get(binaryEvent.getPartitionId());
				if (onePartEvents == null) {
					onePartEvents = new ArrayList<>();
					partEvents.put(
						binaryEvent.getPartitionId(),
						onePartEvents
					);
				}
				onePartEvents.add(binaryEvent);
			}
			for (Map.Entry<Integer, List<BinaryEvent>> entry : partEvents.entrySet()) {
				int partitionId = entry.getKey();
				List<BinaryEvent> events = entry.getValue();
				RdsPacket.RdsMessage.Builder msgBuilder = RdsPacket.RdsMessage.newBuilder();
				int size = 0;
				for (BinaryEvent binaryEvent : events) {
					assert binaryEvent.getEncodedData() instanceof byte[];
					size += binaryEvent.getSize();
					msgBuilder.addMessages(ByteString.copyFrom((byte[]) binaryEvent.getEncodedData()));
				}
				msgBuilder.setMessageCount(events.size())
					.setMessageSize(size);
				partMsgBuilder.putPartitionMessages(
					partitionId,
					msgBuilder.build()
				);
			}

			BinlogInfo lastestBinlogInfo = binaryEvents.get(binaryEvents.size() - 1).getBinlogInfo();

			GeneratedMessageV3 checkpointMessage = lastestBinlogInfo.toCheckpoint();
			if (SourceType.MySQL.equals(sourceType)) {
				envelopeBuilder.setLatestCheckpoint((RdsPacket.Checkpoint) checkpointMessage);
			} else {
				throw new IllegalArgumentException();
			}
			envelopeBuilder.setMessageType(RdsPacket.MessageType.PARTITIONEDMESSAGE).setMessage(partMsgBuilder.build().toByteString());
		}
		return envelopeBuilder.build().toByteArray();
	}
}
