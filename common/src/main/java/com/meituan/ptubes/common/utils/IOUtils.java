package com.meituan.ptubes.common.utils;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Objects;

public class IOUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

    public static void closeQuietly(RandomAccessFile file) {
        if (Objects.nonNull(file)) {
            try {
                file.close();
            } catch (IOException ioe) {
                LOG.error("close file error", ioe);
            }
        } else {
            return;
        }
    }

}
