package com.meituan.ptubes.reader.container.common.config.handler;

import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.common.constants.ProducerConstants;
import java.io.File;
import java.io.IOException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import org.apache.commons.io.FileUtils;


public class MetaFileReaderWriter {

    private static final Logger LOG = LoggerFactory.getLogger(MetaFileReaderWriter.class);

    public static String getProducerBaseDir(String readerTaskName) {
        return ContainerConstants.BASE_DIR + "/" + readerTaskName + "/" + ProducerConstants.CONF_BASE_DIR;
    }

    public static void backupProducerDir(String readerTaskName) {
        String baseDir = getProducerBaseDir(readerTaskName);
        File file = new File(baseDir);
        if (file.exists()) {
            try {
                // Attention: The parent directory is the same, renameTo is valid
                FileUtils.moveDirectory(
                    file,
                    new File(baseDir + "." + System.currentTimeMillis())
                );
            } catch (IOException e) {
                LOG.error(
                    "backup {} error",
                    baseDir,
                    e
                );
            }
        }
    }

}
