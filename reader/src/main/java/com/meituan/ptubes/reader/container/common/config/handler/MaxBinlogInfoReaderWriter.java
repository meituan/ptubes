package com.meituan.ptubes.reader.container.common.config.handler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerBaseConfig;
import com.meituan.ptubes.reader.container.common.vo.Gtid;
import com.meituan.ptubes.reader.container.common.vo.MySQLMaxBinlogInfo;


public class MaxBinlogInfoReaderWriter extends AbstractMaxBinlogInfoReaderWriter<MySQLMaxBinlogInfo> {

	public MaxBinlogInfoReaderWriter(String producerName,
		ProducerBaseConfig producerBaseConfig, MySQLMaxBinlogInfo defaultMaxBinlogInfo) {
		super(producerName, producerBaseConfig, defaultMaxBinlogInfo);
	}

	@Override
	protected MySQLMaxBinlogInfo parseFrom(List<String> maxBinlogInfoContent) {
		Map<String, String> contentMap = new HashMap<>();
		MySQLMaxBinlogInfo mySQLMaxBinlogInfo = new MySQLMaxBinlogInfo();
		for (String str : maxBinlogInfoContent) {
			String[] kv = str.split("=");
			if (kv.length >= 2) {
				contentMap.put(kv[0].trim(), kv[1].trim());
			} else {
				contentMap.put(kv[0].trim(), "");
			}
		}
		mySQLMaxBinlogInfo.setChangeId(Integer.parseInt(contentMap.get("changeId")));
		mySQLMaxBinlogInfo.setServerId(Long.parseLong(contentMap.get("serverId")));
		mySQLMaxBinlogInfo.setBinlogId(Integer.parseInt(contentMap.get("binlogId")));
		mySQLMaxBinlogInfo.setBinlogOffset(Long.parseLong(contentMap.get("binlogOffset")));
		mySQLMaxBinlogInfo.setEventIndex(Long.parseLong(contentMap.get("eventIndex")));
		mySQLMaxBinlogInfo.setGtid(new Gtid(contentMap.get("gtid")));
		mySQLMaxBinlogInfo.setGtidSet(contentMap.get("gtidSet"));
		mySQLMaxBinlogInfo.setBinlogTime(Long.parseLong(contentMap.get("binlogTime")));
		mySQLMaxBinlogInfo.setRefreshTime(Long.parseLong(contentMap.get("refreshTime")));
		return mySQLMaxBinlogInfo;
	}

}
