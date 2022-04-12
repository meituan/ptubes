/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package com.meituan.ptubes.reader.storage.mem.buffer;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

public class PtubesEventBufferMetaInfo {
	private static final Logger LOG = LoggerFactory.getLogger(PtubesEventBufferMetaInfo.class);

	// these are keys in meta file info. Each value matches the corresponding setting
	// in the DbusEventBuf.
	public static final String EVENT_STATE = "eventState";
	public static final String TIMESTAMP_OF_LATEST_DATA_EVENT = "timestampOfLatestDataEvent";
	public static final String TIMESTAMP_OF_FIRST_EVENT = "timestampOfFirstEvent";
	public static final String PREV_BINLOGINFO = "prevBinlogInfo";
	public static final String SEEN_END_OF_PERIOD_BINLOGINFO = "seenEndOfPeriodBinlogInfo";
	public static final String LAST_WRITTEN_BINLOGINFO = "getLastWrittenBinlogInfo";
	public static final String NUM_EVENTS_IN_WINDOW = "numEventsInWindow";
	public static final String EVENT_START_INDEX = "eventStartIndex";
	public static final String ALLOCATED_SIZE = "allocatedSize";
	public static final String BUFFER_EMPTY = "empty";
	public static final String BUFFER_TAIL = "tail";
	public static final String BUFFER_HEAD = "head";
	public static final String MAX_BUFFER_SIZE = "maxBufferSize";
	public static final String CURRENT_WRITE_POSITION = "currentWritePosition";
	public static final String BYTE_BUFFER_INFO = "ByteBufferInfo";
	public static final String NUM_BYTE_BUFFER = "ByteBufferNum";

	/**
	 * helper class for buffer serialization
	 */
	public static class BufferInfo {
		public static final String DELIMITER = ",";
		public int pos;
		public int limit;
		public int cap;

		public BufferInfo(int pos, int limit, int cap) {
			this.pos = pos;
			this.limit = limit;
			this.cap = cap;
		}

		BufferInfo(String fromString) throws
				PtubesEventBufferMetaInfoException { // string format is "pos,limit,capacity"
			String[] info = fromString.split(DELIMITER);
			if (info.length != 3) {
                throw new PtubesEventBufferMetaInfoException(
                        "parsing BufferInfo failed for " + fromString);
            }
			try {
				pos = Integer.parseInt(info[0]);
				limit = Integer.parseInt(info[1]);
				cap = Integer.parseInt(info[2]);
			} catch (NumberFormatException e) {
				throw new PtubesEventBufferMetaInfoException(
						"parsing BufferInfo failed for " + fromString + " " + e.getLocalizedMessage());
			}
		}

		public int getLimit() {
			return limit;
		}

		public int getPos() {
			return pos;
		}

		public int getCapacity() {
			return cap;
		}

		@Override
		public String toString() {
			return pos + DELIMITER + limit + DELIMITER + cap;
		}
	}

	public static class PtubesEventBufferMetaInfoException extends IOException {
		public PtubesEventBufferMetaInfoException(String string) {
			super(string);
		}

		public PtubesEventBufferMetaInfoException(PtubesEventBufferMetaInfo mi, String string) {
			super("[" + mi.toString() + "]:" + string);
		}

		private static final long serialVersionUID = 1L;
	}

	private static final int META_INFO_VERSION = 1;
	private static final char KEY_VALUE_SEP = ' ';
	private final Map<String, String> info = new HashMap<String, String>(100);
	private boolean valid = false;
	private final File file;

	public PtubesEventBufferMetaInfo(File metaFile) {
		file = metaFile;
	}

	public boolean isValid() {
		return valid;
	}

	public String getSessionId() {
		return getVal("sessionId");
	}

	public void setSessionId(String sid) {
		setVal("sessionId", sid);
	}

	public PtubesEventBufferMetaInfo.BufferInfo getReadBufferInfo()
			throws PtubesEventBufferMetaInfoException {
		return new PtubesEventBufferMetaInfo.BufferInfo(getVal("readBufInfo"));
	}

	public PtubesEventBufferMetaInfo.BufferInfo getBinlogInfoIndexBufferInfo()
			throws PtubesEventBufferMetaInfoException {
		return new PtubesEventBufferMetaInfo.BufferInfo(getVal("binlogInfoIndexBufferInfo"));
	}

	public void setBinlogInfoIndexBufferInfo(PtubesEventBufferMetaInfo.BufferInfo bi) {
		setVal("binlogInfoIndexBufferInfo", bi.toString());
	}

	public void setReadBufferInfo(PtubesEventBufferMetaInfo.BufferInfo bi) {
		setVal("readBufInfo", bi.toString());
	}

	public void setVal(String key, String val) {
		info.put(key, val);
	}

	public String getVal(String key) {
		String val = info.get(key);
		return val;
	}

	public long getLong(String key) throws PtubesEventBufferMetaInfoException {
		long l;
		try {
			l = Long.parseLong(getVal(key));
		} catch (NumberFormatException e) {
			throw new PtubesEventBufferMetaInfoException(this,
					"getKey=" + key + " msg= " + e.getLocalizedMessage());
		}
		return l;
	}

	public int getInt(String key) throws PtubesEventBufferMetaInfoException {
		int i;
		try {
			i = Integer.parseInt(getVal(key));
		} catch (NumberFormatException e) {
			throw new PtubesEventBufferMetaInfoException(this, e.getLocalizedMessage());
		}
		return i;
	}

	public boolean getBool(String key) {
		return Boolean.parseBoolean(getVal(key));
	}

	/**
	 * load getKey/values form the file
	 * line is separated by the 'space'
	 *
	 * @throws PtubesEventBufferMetaInfoException
	 */
	public boolean loadMetaInfo() throws PtubesEventBufferMetaInfoException {
		//
		BufferedReader br = null;
		valid = false;
		boolean debugEnabled = PtubesEventBuffer.LOG.isDebugEnabled();
		try {
			InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
			br = new BufferedReader(isr);

			PtubesEventBuffer.LOG.info("loading metaInfoFile " + file);
			info.clear();
			String line = br.readLine();
			while (line != null) {
				if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }

				// format is getKey' 'val
				int idx = line.indexOf(KEY_VALUE_SEP);
				if (idx < 0) {
					PtubesEventBuffer.LOG.warn("illegal line in metaInfoFile. line=" + line);
					continue;
				}
				String key = line.substring(0, idx);
				String val = line.substring(idx + 1);
				info.put(key, val);
				if (debugEnabled) {
                    PtubesEventBuffer.LOG.debug("\tgetKey=" + key + "; val=" + val);
                }
				line = br.readLine();
				valid = true;
			}
		} catch (IOException e) {
			throw new PtubesEventBufferMetaInfoException(this,
					"cannot read metaInfoFile:  " + e.getLocalizedMessage());
		} finally {
			if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    PtubesEventBuffer.LOG.warn("faild to close " + file);
                }
            }
		}

		int version = getInt("version");
		if (version != META_INFO_VERSION) {
            throw new PtubesEventBufferMetaInfoException(this,
                    "metaInfoFile version doesn't match. Please remove the metafile and restart");
        }

		if (isMetaFileOlderThenMMappedFiles(getSessionId())) {
			// we do not act on this, but keep it as a warning in the logs
			//_valid = false; //not valid file - don't use
		}

		return valid;
	}

	/**
	 * @param sessionId
	 * @return true if mmaped fiels have changed after metaInfo file
	 */
	private boolean isMetaFileOlderThenMMappedFiles(String sessionId) {

		if (sessionId == null) {
            return false;// valid case, no session is ok
        }

		// one extra check is to verify that the session directory that contains the actual buffers
		// was not modified after the file was saved
		long metaFileModTime = file.lastModified();
		LOG.debug(file + " mod time: " + metaFileModTime);

		// check the directory first
		File sessionDir = new File(file.getParent(), sessionId);
		long sessionDirModTime = sessionDir.lastModified();
		if (sessionDirModTime > metaFileModTime) {
			LOG.error("Session dir " + sessionDir +
					" seemed to be modified AFTER metaFile " + file +
					" dirModTime=" + sessionDirModTime + "; metaFileModTime=" + metaFileModTime);
			return true;
		}

		// check each file in the directory
		String[] mmappedFiles = sessionDir.list();
		if (mmappedFiles == null) {
			LOG.error("There are no mmaped files in the session directory: " + sessionDir);
			return true;
		}

		for (String fName : mmappedFiles) {
			File f = new File(sessionDir, fName);
			long modTime = f.lastModified();
			LOG.debug(f + " mod time: " + modTime);
			if (modTime > metaFileModTime) {
				LOG.error("MMapped file " + f + "(" + modTime + ") seemed to be modified AFTER metaFile " + file + "("
						+ metaFileModTime + ")");
				return true;
			}
		}
		return false;// valid file - go ahead and use it
	}

	/**
	 * reads cap, pos and limit for each buffer
	 *
	 * @return
	 * @throws PtubesEventBufferMetaInfoException
	 */
	public PtubesEventBufferMetaInfo.BufferInfo[] getBuffersInfo()
			throws PtubesEventBufferMetaInfoException {
		int bufNum = 0;
		try {
			bufNum = Integer.parseInt(info.get("ByteBufferNum"));
		} catch (NumberFormatException e) {
			throw new PtubesEventBufferMetaInfoException(this, e.getLocalizedMessage());
		}
		String bufInfoAll = info.get("ByteBufferInfo");

		String[] buffersInfo = bufInfoAll.split(" ");
		if (buffersInfo.length != bufNum) {
			throw new PtubesEventBufferMetaInfoException(this,
					"bufNum " + bufNum + " doesn't match bufInfo size [" + bufInfoAll + "]");
		}
		PtubesEventBufferMetaInfo.BufferInfo[] bInfos = new PtubesEventBufferMetaInfo.BufferInfo[bufNum];
		for (int i = 0; i < buffersInfo.length; i++) {
			bInfos[i] = new PtubesEventBufferMetaInfo.BufferInfo(buffersInfo[i]);
		}
		return bInfos;
	}

	public void saveAndClose() throws IOException {
		// update version
		setVal("version", Integer.toString(META_INFO_VERSION));

		if (file.exists()) {
			// Attention: The parent directory is the same, renameTo is valid
			File renameTo = new File(file.getAbsoluteFile() + "." + System.currentTimeMillis());
			if (!file.renameTo(renameTo)) {
				PtubesEventBufferMetaInfo.LOG.warn("failed to rename " + file + " to " + renameTo);
			}
			PtubesEventBuffer.LOG.warn("metaInfoFile " + file + " exists. it is renambed to " + renameTo);
		}
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
		BufferedWriter bw = new BufferedWriter(osw);

		try {
			for (Map.Entry<String, String> e : info.entrySet()) {
				bw.write(e.getKey() + KEY_VALUE_SEP + e.getValue());
				bw.newLine();
			}
		} finally {
			if (bw != null) {
                bw.close();
            }
		}
	}

	@Override
	public String toString() {
		return file.getAbsolutePath();
	}
}
