package com.meituan.ptubes.common.utils;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class PropertiesUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesUtils.class);
    public static final String CLASSPATH_URL_PREFIX = "classpath:";

    public static Properties getProperties(String fileName) {
        Properties properties = new Properties();
        try {

            InputStream in = PropertiesUtils.class.getClassLoader()
                    .getResourceAsStream(fileName);
            properties = new Properties();
            properties.load(in);
        } catch (IOException e) {
            LOG.error("read Properties file failed, fileName = {}", fileName);
        }
        return properties;
    }

    public static Properties getPropertiesByFile(String filePath) {
        Properties fileProperties = new Properties();

        if (filePath.startsWith(CLASSPATH_URL_PREFIX)) {
            filePath = StringUtils.substringAfter(filePath, CLASSPATH_URL_PREFIX);
            fileProperties = getProperties(filePath);
        } else {
            try {
                fileProperties.load(new FileInputStream(filePath));
            } catch (IOException e) {
                LOG.error("load conf failed, conf = {}", filePath, e);
            }
        }

        return fileProperties;
    }
}
