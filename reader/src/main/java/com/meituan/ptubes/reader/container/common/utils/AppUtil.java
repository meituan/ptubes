package com.meituan.ptubes.reader.container.common.utils;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.IPUtil;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class AppUtil {

    private static final Logger LOG = LoggerFactory.getLogger(AppUtil.class);
    private static final String VERSION = "1.5.0";

    public static volatile String APP_NAME = null;
    public static String IP = IPUtil.getIpV4();
    public static Object objectLock = new Object();

    public static String getAppName() {
        if (APP_NAME == null) {
            synchronized (objectLock) {
                if (APP_NAME == null) {
                    InputStream appkeyStream = null;
                    try {
                        // java -D can also be used, but app.properties can unify other components, which has certain advantages. app.properties can be generated from files
                        appkeyStream = AppUtil.class.getClassLoader()
                            .getResourceAsStream("META-INF/app.properties");
                        Properties props = new Properties();
                        props.load(appkeyStream);
                        APP_NAME = props.getProperty("app.name");
                        
                    } catch (Exception e) {
                        throw new PtubesRunTimeException(
                            "load app name fail",
                            e
                        );
                    } finally {
                        if (appkeyStream != null) {
                            try {
                                appkeyStream.close();
                            } catch (IOException ioe) {
                                LOG.error("close appkey Stream fail", ioe);
                            }
                        }
                    }

                    if (StringUtils.isBlank(APP_NAME)) {
                        throw new PtubesRunTimeException("app name is not exist");
                    }
                }
                return APP_NAME;
            }
        } else {
            return APP_NAME;
        }
    }
    public static String getVersion() {
        return VERSION;
    }
}
