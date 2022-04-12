package com.meituan.ptubes.storage;

import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.thread.PtubesThreadBase;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntryFactory;
import com.meituan.ptubes.storage.utils.DbChangeEntryUtil;
import com.meituan.ptubes.storage.utils.TableUtil;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.storage.channel.WriteChannel;

public class WriteTask extends PtubesThreadBase {
	private volatile WriteChannel writeChannel;
	private volatile int eventCount = 0;
	private MySQLBinlogInfo startBinlogInfo;

	public WriteTask(String name, WriteChannel writeChannel, MySQLBinlogInfo startBinlogInfo) {
		super(name);
		this.writeChannel = writeChannel;
		this.startBinlogInfo = startBinlogInfo;
	}

	public int getEventCount() {
		return eventCount;
	}

	public void switchStorageMode(StorageConstant.StorageMode storageMode) throws InterruptedException {
		writeChannel.switchStorageMode(storageMode);
		if (writeChannel.getStorageMode() != storageMode) {
			throw new AssertionError("SwitchStorageMode error");
		}
	}

	@Override
	public void run() {
		log.info("WriteTask Thread start");
		while (!isShutdownRequested()) {
			if (isPauseRequested()) {
				log.info("Pause requested for WriteTask. Pausing !!");
				signalPause();
				log.info("Pausing. Waiting for resume command");
				try {
					awaitUnPauseRequest();
				} catch (InterruptedException e) {
					log.info("WriteTask {} Interrupted !", getName(), e);
				}
				log.info("Resuming WriteTask !!");
				signalResumed();
				log.info("WriteTask resumed !!");
			}

			eventCount++;
			try {
				appendEvent();
			} catch (Exception e) {
				log.error("Append event error", e);
				eventCount--;
			}
			try {
				sleep(100);
			} catch (InterruptedException e) {
				log.info("Sleep InterruptedException");
			}
		}
		log.info("WriteTask Thread done");
		doShutdownNotify();
	}

	public void appendEvent() throws Exception {
		// append data
		MySQLBinlogInfo dataBinlogInfo = new MySQLBinlogInfo(startBinlogInfo.getChangeId(), startBinlogInfo.getServerId(),
				startBinlogInfo.getBinlogId(), startBinlogInfo.getBinlogOffset() + eventCount,
				startBinlogInfo.getUuid(), startBinlogInfo.getTxnId() + eventCount, 0, System.currentTimeMillis());
		ChangeEntry mySQLChangeEntry = DbChangeEntryUtil.genRandomDbChangeEntry(dataBinlogInfo,
				TableUtil.TEST01_TABLE_NAME);
		this.writeChannel.append(mySQLChangeEntry);

		// append commit
		MySQLBinlogInfo commitBinlogInfo = new MySQLBinlogInfo(startBinlogInfo.getChangeId(), startBinlogInfo.getServerId(),
				startBinlogInfo.getBinlogId(), startBinlogInfo.getBinlogOffset() + eventCount,
				startBinlogInfo.getUuid(), startBinlogInfo.getTxnId() + eventCount, 1, System.currentTimeMillis());

		ChangeEntry commitEntry = ChangeEntryFactory.createCommitEntry(commitBinlogInfo);
		this.writeChannel.append(commitEntry);
	}
}
