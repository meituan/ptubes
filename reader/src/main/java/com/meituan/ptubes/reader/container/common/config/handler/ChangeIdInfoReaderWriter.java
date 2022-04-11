package com.meituan.ptubes.reader.container.common.config.handler;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.constants.ProducerConstants;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.utils.FileUtil;
import com.meituan.ptubes.reader.container.common.vo.ChangeIdInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;


public class ChangeIdInfoReaderWriter {
	private static final Logger LOG = LoggerFactory.getLogger(ChangeIdInfoReaderWriter.class);
	private final Logger log;
	private static final String TEMP = ".temp";
	private final String readerTaskName;
	private final String fileDir;
	private List<ChangeIdInfo> changeIdInfos = new ArrayList<>();

	public ChangeIdInfoReaderWriter(String readerTaskName) {
		this.readerTaskName = readerTaskName;
		this.log = LoggerFactory.getLogger(ChangeIdInfoReaderWriter.class + "_" + readerTaskName);
		this.fileDir = getProducerBaseDir(readerTaskName) + "/"  + ProducerConstants.CHANGE_ID_INFO_FILE_NAME;
		loadInitialValue();
	}

	private String getProducerBaseDir(String name) {
		String baseDir = MetaFileReaderWriter.getProducerBaseDir(name);
		File file = new File(baseDir);
		if (!file.exists()) {
			file.mkdirs();
		}
		return baseDir;
	}

	public static void fillChangeId(String name, MySQLBinlogInfo binlogInfo)
		throws LessThanStorageRangeException {
		ChangeIdInfoReaderWriter changeIdInfoReader = new ChangeIdInfoReaderWriter(name);
		try {
			short changeId = changeIdInfoReader.getChangeId(binlogInfo);
			binlogInfo.setChangeId(changeId);
		} catch (PtubesException e) {
			if (binlogInfo.getServerId() <= 0) {
				throw new LessThanStorageRangeException(e.getMessage());
			}
			// If there is an exception, there may be a backtracking on the reader side, and the corresponding changeId cannot be found according to the serverId.
			// You can set the serverId to -1 and search again according to the time
			LOG.info("origin binlogInfo: {}", binlogInfo);
			binlogInfo.setServerId(-1);
			binlogInfo.setBinlogId(-1);
			binlogInfo.setBinlogOffset(-1);
			binlogInfo.setEventIndex(-1);
			LOG.info("get changeId by time, binlogInfo: {}", binlogInfo);
			short changeId = 0;
			try {
				changeId = changeIdInfoReader.getChangeId(binlogInfo);
			} catch (PtubesException exp) {
				throw new LessThanStorageRangeException(exp.getMessage());
			}
			binlogInfo.setChangeId(changeId);
		}
	}

	public short getCurrentChangeId() {
		if (changeIdInfos.isEmpty()) {
			return 0;
		} else {
			return changeIdInfos.get(changeIdInfos.size() - 1).getChangeId();
		}
	}

	public short getNextChangeId() {
		if (changeIdInfos.isEmpty()) {
			return 0;
		} else {
			return (short) (changeIdInfos.get(changeIdInfos.size() - 1).getChangeId() + 1);
		}
	}

	public short getChangeId(MySQLBinlogInfo binlogInfo) throws PtubesException {
		for (int i = changeIdInfos.size() - 1; i >= 0; i--) {
			ChangeIdInfo changeIdInfo = changeIdInfos.get(i);
			if (binlogInfo.getServerId() == changeIdInfo.getServerId()) {
				if (changeIdInfo.getBeginBinlogId() > 0 && changeIdInfo.getBeginBinlogOffset() > 0) {
					// If binlogId and offset are valid, compare binlogId and offset
					if (binlogInfo.getBinlogId() >= changeIdInfo.getBeginBinlogId()
							&& binlogInfo.getBinlogOffset() >= changeIdInfo.getBeginBinlogOffset()) {
						return changeIdInfo.getChangeId();
					}
				} else if (binlogInfo.getTimestamp() >= changeIdInfo.getBeginTimeStampMS()) {
					// If binlogId and offset are invalid, compare time
					return changeIdInfo.getChangeId();
				}
			} else if (binlogInfo.getServerId() <= 0){
				// There is no serverId, find the point according to the time
				if (binlogInfo.getTimestamp() > 0 && changeIdInfo.getBeginTimeStampMS() > 0 &&
					binlogInfo.getTimestamp() >= changeIdInfo.getBeginTimeStampMS()) {
					return changeIdInfo.getChangeId();
				}
			}
		}
		log.warn("Get changeId error, binlogInfo: {}, changeIdInfos: {}", binlogInfo, changeIdInfos.toString());
		throw new PtubesException(
			"Fill changeId error, binlogInfo: " + binlogInfo + ", changeIdInfos: " + changeIdInfos.toString());
	}


	/**
	 * Load changId related information
	 * 1. If the local file exists, load it, each line is a set of information
	 * 2. The format is similar to changeId=XX,serverId=XXX,gtidSet=XXXX,beginBinlogFile=XXX,beginBinlogOffset=XXX
	 */
	public void loadInitialValue() {
		File changeIdInfoFile = new File(fileDir);
		File tmpFile = new File(fileDir + TEMP);
		List<String> changeIdInfoContent = new ArrayList<>();
		if (changeIdInfoFile.exists()) {
			changeIdInfoContent = FileUtil.readFile(changeIdInfoFile);
		}
		if ((changeIdInfoContent == null || changeIdInfoContent.isEmpty()) && tmpFile.exists()) {
			changeIdInfoContent = FileUtil.readFile(tmpFile);
		}
		if (changeIdInfoContent == null || changeIdInfoContent.isEmpty()) {
			return;
		}

		try {
			for (String str : changeIdInfoContent) {
				Map<String, String> contentMap = new HashMap<>();
				String[] params = str.split(";");
				for (String param : params) {
					String[] kv = param.split("=");
					if (kv.length >= 2) {
						contentMap.put(kv[0].trim(), kv[1].trim());
					} else {
						contentMap.put(kv[0].trim(), "");
					}
				}
				ChangeIdInfo changeIdInfo = new ChangeIdInfo();
				changeIdInfo.setChangeId(Short.parseShort(contentMap.get("changeId")));
				changeIdInfo.setServerId(Long.parseLong(contentMap.get("serverId")));
				changeIdInfo.setBeginBinlogId(Integer.parseInt(contentMap.get("beginBinlogId")));
				changeIdInfo.setBeginBinlogOffset(Long.parseLong(contentMap.get("beginBinlogOffset")));
				changeIdInfo.setPreviousGtidSet(contentMap.get("previousGtidSet"));
				changeIdInfo.setBeginTimeStampMS(Long.parseLong(contentMap.get("beginTimeStampMS")));
				changeIdInfos.add(changeIdInfo);
			}
		} catch (Exception e) {
			throw new PtubesRunTimeException("Init changeIdInfo error: " + e);
		}
	}

	public void addChangeIdInfo(ChangeIdInfo changeIdInfo) throws IOException {
		if (!changeIdInfos.isEmpty() && changeIdInfos.get(changeIdInfos.size() -1).getChangeId() == changeIdInfo.getChangeId()) {
			return;
		}
		log.info("Add new changeIdInfo: " + changeIdInfo);
		changeIdInfos.add(changeIdInfo);

		File tempChangeIdInfoFile = new File(fileDir + TEMP);
		File changeIdInfoFile = new File(fileDir);
		// Attention: The parent directory is the same, renameTo is valid
		if (changeIdInfoFile.exists() && !changeIdInfoFile.renameTo(tempChangeIdInfoFile)) {
			log.error("Unable to backup changeIdInfo file");
		}

		if (!changeIdInfoFile.createNewFile()) {
			log.error("Unable to create new changeIdInfo file:" + tempChangeIdInfoFile.getAbsolutePath());
		}

		FileWriter writer = null;
		try {
			writer = new FileWriter(changeIdInfoFile);
			writer.write(genChangeIdInfoFileContent());
			writer.flush();
			if (log.isDebugEnabled()) {
				log.debug("changeIdInfo persisted: {}", changeIdInfo.toString());
			}
		} finally {
			if (null != writer) {
				writer.close();
			}
		}
	}

	private String genChangeIdInfoFileContent() {
		StringBuilder sb = new StringBuilder();
		for (ChangeIdInfo changeIdInfo : changeIdInfos) {
			sb.append(changeIdInfo.toString()).append("\n");
		}
		return sb.toString();
	}
}
