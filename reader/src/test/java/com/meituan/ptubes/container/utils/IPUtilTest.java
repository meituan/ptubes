package com.meituan.ptubes.container.utils;

import com.meituan.ptubes.common.utils.IPUtil;
import org.junit.Test;

public class IPUtilTest {

    @Test
    public void IPUtilTest() {
        String[] tcs = new String[] {
            "192.168.0.1",
            "127.0.0.1",
            "127.0.0.2"
        };

        for (String tc : tcs) {
            long num = IPUtil.ipToLong(tc);
            String numToIP = IPUtil.longToIp(num, false);
            System.out.println(numToIP);
            assert tc.equals(numToIP);
        }
    }

}
