package com.meituan.ptubes.common.utils;

import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public class PbJsonUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PbJsonUtil.class);
    public static String printToStringDefaultNull(Message message) {
        try {
            return JsonFormat.printToString(message);
        } catch (Exception e) {
            LOG.error("caught error in printToStringDefaultNull", e);
        }
        return null;
    }
}
