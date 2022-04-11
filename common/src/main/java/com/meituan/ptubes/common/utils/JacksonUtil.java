package com.meituan.ptubes.common.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.io.IOException;

public class JacksonUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JacksonUtil.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String EMPTY_STRING = "";

    public static <T> T fromJson(
        String json,
        Class<T> classOfT
    ) {
        try {
            return OBJECT_MAPPER.readValue(
                json,
                classOfT
            );
        } catch (IOException ex) {
            LOG.error(
                "Parse json string error.",
                ex
            );
        }

        return null;
    }

    public static String toJson(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException ex) {
            LOG.error(
                "Build json string error.",
                ex
            );
        }

        return EMPTY_STRING;
    }
}
