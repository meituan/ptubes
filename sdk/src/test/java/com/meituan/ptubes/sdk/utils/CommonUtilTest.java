package com.meituan.ptubes.sdk.utils;

import java.util.Date;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;
import org.junit.Test;

public class CommonUtilTest {
    @Test
    public void testTransformToPhysicalTime() {
        long physicalTime = 1630661830000L;
        assert physicalTime == CommonUtil.transformToPhysicalTime(physicalTime, RdsCdcSourceType.MYSQL);
    }

    @Test
    public void toolParseLogicTimeToPhysicalTime() {
        // long logicTime = 427694625871364097L;
        long logicTime = 428504325499912261L;
        long physicalTime = logicTime >> 18;
        System.out.println(physicalTime);
        System.out.println(new Date(physicalTime));
    }

}
