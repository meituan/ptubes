package com.meituan.ptubes.reader.container.network.request.sub;

import com.meituan.ptubes.reader.container.network.encoder.EncoderType;
import com.meituan.ptubes.reader.container.network.request.Token;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.sdk.model.PartitionClusterInfo;
import com.meituan.ptubes.sdk.model.ServiceGroupInfo;


public class SubRequest<CHECK_POINT extends BuffaloCheckpoint> extends Token {

	/**default**/
	private EncoderType codec = EncoderType.PROTOCOL_BUFFER;
	private CHECK_POINT buffaloCheckpoint;
	private ServiceGroupInfo serviceGroupInfo;
	/** partitionSet range: [0, partitionTotal-1] **/
	private PartitionClusterInfo partitionClusterInfo;
	private boolean needDDL = true;
	private boolean needEndTransaction = true;

	public EncoderType getCodec() {
		return codec;
	}

	public void setCodec(EncoderType codec) {
		this.codec = codec;
	}

	public CHECK_POINT getBuffaloCheckpoint() {
		return buffaloCheckpoint;
	}

	public void setBuffaloCheckpoint(CHECK_POINT buffaloCheckpoint) {
		this.buffaloCheckpoint = buffaloCheckpoint;
	}

	public ServiceGroupInfo getServiceGroupInfo() {
		return serviceGroupInfo;
	}

	public void setServiceGroupInfo(ServiceGroupInfo serviceGroupInfo) {
		this.serviceGroupInfo = serviceGroupInfo;
	}

	public PartitionClusterInfo getPartitionClusterInfo() {
		return partitionClusterInfo;
	}

	public void setPartitionClusterInfo(PartitionClusterInfo partitionClusterInfo) {
		this.partitionClusterInfo = partitionClusterInfo;
	}

	public boolean isNeedDDL() {
		return needDDL;
	}

	public void setNeedDDL(boolean needDDL) {
		this.needDDL = needDDL;
	}

	public boolean isNeedEndTransaction() {
		return needEndTransaction;
	}

	public void setNeedEndTransaction(boolean needEndTransaction) {
		this.needEndTransaction = needEndTransaction;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[ codec=").append(codec.name())
				.append(", subscriptionInfo=").append(serviceGroupInfo == null ? "" : JSONUtil.toJsonStringSilent(serviceGroupInfo, true))
				.append(", partitionInfo=").append(partitionClusterInfo == null ? "" : JSONUtil.toJsonStringSilent(partitionClusterInfo, true))
				.append(", checkpoint=").append(buffaloCheckpoint == null ? "" : JSONUtil.toJsonStringSilent(buffaloCheckpoint, true))
				.append(", needDdlEvent=").append(needDDL)
				.append(", needTxnEndEvent=").append(needEndTransaction).append(" ]");

		return sb.toString();
	}
}
