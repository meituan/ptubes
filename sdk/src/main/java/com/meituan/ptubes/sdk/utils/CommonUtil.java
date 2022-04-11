package com.meituan.ptubes.sdk.utils;

import com.meituan.ptubes.sdk.config.RdsCdcSourceType;
import com.meituan.ptubes.sdk.constants.SdkConstants;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import com.meituan.ptubes.common.utils.PropertiesUtils;

public class CommonUtil {
    public static long transformToPhysicalTime(long logicTime, RdsCdcSourceType sourceType) {
        switch (sourceType) {
            case MYSQL:
            default:
                return logicTime;
        }
    }

    public static boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (Throwable tr) {
            return false;
        }
    }

    public static Set<String> buildSdkTaskSet(String path) {
        String rawSdkTaskSet = null;
        if (null == path || path.isEmpty()) {
            rawSdkTaskSet = PropertiesUtils
                    .getPropertiesByFile(PropertiesUtils.CLASSPATH_URL_PREFIX + SdkConstants.SDK_CONFIG_SET_FILE_NAME)
                    .getProperty("ptubes.sdk.task.set");
        } else {
            rawSdkTaskSet = PropertiesUtils
                    .getPropertiesByFile(path)
                    .getProperty("ptubes.sdk.task.set");
        }
        Set<String> sdkTaskSet = new HashSet<>(Arrays.asList(rawSdkTaskSet.split(",")));
        return sdkTaskSet;
    }

}
