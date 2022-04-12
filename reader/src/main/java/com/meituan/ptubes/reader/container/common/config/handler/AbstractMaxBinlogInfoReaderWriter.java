package com.meituan.ptubes.reader.container.common.config.handler;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.reader.container.common.constants.ProducerConstants;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerBaseConfig;
import com.meituan.ptubes.common.utils.FileUtil;
import com.meituan.ptubes.reader.container.common.vo.MaxBinlogInfo;

public abstract class AbstractMaxBinlogInfoReaderWriter<MAX_BINLOG_INFO extends MaxBinlogInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMaxBinlogInfoReaderWriter.class);
    private static final String TEMP = ".temp";

    private final String producerName;
    private final String fileDir;
    private final ProducerBaseConfig producerBaseConfig;

    // Record where the last binLog was consumed
    private volatile MAX_BINLOG_INFO lastSaveMaxBinlogInfo;
    private long lastSaveTimestamp = -1;

    public AbstractMaxBinlogInfoReaderWriter(String producerName, ProducerBaseConfig producerBaseConfig, MAX_BINLOG_INFO defaultMaxBinlogInfo) {
        this.producerName = producerName;
        this.producerBaseConfig = producerBaseConfig;
        this.fileDir = getProducerBaseDir(producerName) + "/"  + ProducerConstants.MAX_BINLOG_INFO_FILE_NAME;
        loadInitialValue(defaultMaxBinlogInfo);
    }

    private String getProducerBaseDir(String name) {
        String baseDir = MetaFileReaderWriter.getProducerBaseDir(name);
        File file = new File(baseDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        return baseDir;
    }

    public MAX_BINLOG_INFO getLastSaveMaxBinlogInfo() {
        return lastSaveMaxBinlogInfo;
    }

    public boolean saveMaxBinlogInfoInterval(MAX_BINLOG_INFO maxBinlogInfo) {
        return saveMaxBinlogInfo(maxBinlogInfo, false);
    }

    public void saveMaxBinlogInfoDirectly(MAX_BINLOG_INFO maxBinlogInfo) {
        saveMaxBinlogInfo(maxBinlogInfo, true);
    }

    /**
     * Record MaxBinlogInfo to local meta information
     * @param maxBinlogInfo
     * @return true: MaxBinlogInfo storage succeeded, false: storage failed or the storage interval has not been reached
     */
    public boolean saveMaxBinlogInfo(MAX_BINLOG_INFO maxBinlogInfo) {
        try {
            this.lastSaveMaxBinlogInfo = maxBinlogInfo;
            return saveMaxBinlogInfoInterval(maxBinlogInfo);
        } catch (Exception e) {
            LOG.error("Save MaxBinlogInfo fail, maxBinlogInfo: {}", maxBinlogInfo, e);
        }
        return false;
    }

    abstract MAX_BINLOG_INFO parseFrom(List<String> maxBinlogInfoContent);

	protected void loadInitialValue(MAX_BINLOG_INFO defaultMaxBinlogInfo) {
        File binlogInfoFile = new File(fileDir);
        File tmpFile = new File(fileDir + TEMP);
        List<String> binlogInfoContent = FileUtil.readFile(binlogInfoFile);
        if (binlogInfoContent == null || binlogInfoContent.isEmpty()) {
            binlogInfoContent = FileUtil.readFile(tmpFile);
        }
        if (binlogInfoContent == null || binlogInfoContent.isEmpty()) {
            LOG.error("Initial maxBinlogInfo does not exist, start from currentTime");
            // If there is no configuration file, dumpbinlog from the current time point, serverId and changeId are set to the current serverId and changeId
            lastSaveMaxBinlogInfo = defaultMaxBinlogInfo;
            saveMaxBinlogInfoDirectly(lastSaveMaxBinlogInfo);
            return;
        }

        try {
            lastSaveMaxBinlogInfo = parseFrom(binlogInfoContent);
            return;
        } catch (Exception e) {
            LOG.error("Init maxBinlogInfo error: ", e);
			throw new PtubesRunTimeException("Init maxBinlogInfo error: " + e.getMessage());
        }
    }

    public boolean saveMaxBinlogInfo(MAX_BINLOG_INFO maxBinlogInfo, boolean isDirectly) {
        this.lastSaveMaxBinlogInfo = maxBinlogInfo;
        long curTime = System.currentTimeMillis() / 1000;
        if (isDirectly || curTime - lastSaveTimestamp > producerBaseConfig.getMaxBinlogInfoFlushIntervalSec()) {
            try {
                writeToFile();
                lastSaveTimestamp = curTime;
                return true;
            } catch (Exception e) {
                LOG.error("Save maxBinlogInfo fail", e);
            }
        }
        return false;
    }

    private void writeToFile() throws IOException {
        // delete the temp file if one exists
        File tempBinlogInfoFile = new File(fileDir + TEMP);
        File binlogInfoFile = new File(fileDir);
        // Attention: The parent directory is the same, renameTo is valid
        if (binlogInfoFile.exists() && !binlogInfoFile.renameTo(tempBinlogInfoFile)) {
            LOG.error("Unable to backup maxBinlogInfo file");
        }

        if (!binlogInfoFile.createNewFile()) {
            LOG.error("Unable to create new maxBinlogInfo file:" + tempBinlogInfoFile.getAbsolutePath());
        }

        FileWriter writer = null;
        try {
            writer = new FileWriter(binlogInfoFile);
            writer.write(lastSaveMaxBinlogInfo.toString());
            writer.flush();
            if (LOG.isDebugEnabled()) {
                LOG.debug("MaxBinlogInfo persisted: {}", lastSaveMaxBinlogInfo.toString());
            }
        } finally {
            if (null != writer) {
                writer.close();
            }
        }
    }

}
