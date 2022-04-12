package com.meituan.ptubes.reader.container.config;

import com.meituan.ptubes.reader.container.common.config.ConfigService;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

public class ReaderTaskSetConfigBuilder {

    public static final ReaderTaskSetConfigBuilder BUILDER = new ReaderTaskSetConfigBuilder();

    public Set<String> build(ConfigService configService) {
        String rawReadTaskSet = configService.getConfig(ConfigKeyConstants.ServerConfiguration.TASKS.getKey());
        if (StringUtils.isBlank(rawReadTaskSet)) {
            return Collections.EMPTY_SET;
        }

        Set<String> readTaskSet = new HashSet<>(Arrays.asList(rawReadTaskSet.split(",")));
        return readTaskSet;
    }
}
