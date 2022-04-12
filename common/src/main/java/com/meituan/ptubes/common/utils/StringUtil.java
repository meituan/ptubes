package com.meituan.ptubes.common.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class StringUtil {

    public static String stringListConjunction(Collection<String> collection, String spliter) {
        StringBuilder sb = new StringBuilder();
        List<String> list = new ArrayList<String>();
        list.addAll(collection);
        for (int i = 0; i < list.size(); i++) {
            if (StringUtils.isBlank(list.get(i))) {
                continue;
            }
            sb.append(list.get(i));
            sb.append(spliter);
        }
        String res = "";
        if (sb.length() > 0) {
            res = sb.substring(0, sb.length() - spliter.length());
        }
        return res;
    }

    public static List<String> stringListBreak(String origin, String spliter) {
        return stringListBreak(origin, spliter, true);
    }

    public static List<String> stringListBreak(String origin, String spliter, boolean filterEmptyString) {
        List<String> result = new ArrayList<>();

        if (StringUtils.isBlank(origin) == false) {
            for (String item : origin.split(spliter)) {
                String itemAfterTrim = item.trim();
                if (StringUtils.isNotBlank(origin)) {
                    result.add(itemAfterTrim);
                } else {
                    if (filterEmptyString == false) {
                        result.add(itemAfterTrim);
                    }
                }
            }
        }

        return result;
    }

}
