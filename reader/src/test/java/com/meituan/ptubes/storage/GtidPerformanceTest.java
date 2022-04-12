package com.meituan.ptubes.storage;

import com.meituan.ptubes.reader.container.common.vo.Gtid;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GtidPerformanceTest {

    private static final String tc = "c055a63d-99c7-11ea-a39c-0a580a24b346";
    private byte[] testGtidBytes;

    @Before
    public void init() {
        testGtidBytes = Gtid.getUuidByte(tc);
    }

    @Test
    public void bytesToGtidStrTest() {
        System.out.println(Gtid.getUuidStr(testGtidBytes));
        Assert.assertTrue(tc.equals(Gtid.getUuidStr(testGtidBytes)));
        System.out.println(Gtid.getUuidStrOld(testGtidBytes));
        Assert.assertTrue(tc.equals(Gtid.getUuidStrOld(testGtidBytes)));
    }

}
