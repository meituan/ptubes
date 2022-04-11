package com.meituan.ptubes.sdk.utils;

import java.io.UnsupportedEncodingException;
import java.sql.Types;
import com.meituan.ptubes.common.utils.JdbcTypeUtil;
import org.junit.Assert;
import org.junit.Test;



public class JdbcTypeUtilTest {

    private static final String tableName = "testTableName";
    private static final String columnName = "testColumnName";

    @Test
    public void typeConvertTest() throws UnsupportedEncodingException {
        Assert.assertEquals(
            null,
            typeConvert(
                null,
                Types.INTEGER,
                "INT"
            )
        );
        Assert.assertEquals(
            1,
            typeConvert(
                "1",
                Types.INTEGER,
                "INT"
            )
        );
        Assert.assertEquals(
            (short) 1,
            typeConvert(
                "1",
                Types.SMALLINT,
                "SMALLINT"
            )
        );
        Assert.assertEquals(
            (byte) 1,
            typeConvert(
                "1",
                Types.TINYINT,
                "TINYINT"
            )
        );
        Assert.assertEquals(
            1L,
            typeConvert(
                "1",
                Types.BIGINT,
                "BIGINT"
            )
        );
        Assert.assertEquals(
            true,
            typeConvert(
                "1",
                Types.BOOLEAN,
                "BOOLEAN"
            )
        );
        Assert.assertEquals(
            123.456,
            typeConvert(
                "123.456",
                Types.DOUBLE,
                "DOUBLE"
            )
        );
        Assert.assertEquals(
            123.456,
            typeConvert(
                "123.456",
                Types.FLOAT,
                "FLOAT"
            )
        );
        Assert.assertEquals(
            123.456F,
            typeConvert(
                "123.456",
                Types.REAL,
                "REAL"
            )
        );

        Assert.assertArrayEquals(
            new byte[0],
            (byte[]) typeConvert(
                "",
                Types.BINARY,
                "BINARY"
            )
        );
        Assert.assertArrayEquals(
            new byte[0],
            (byte[]) typeConvert(
                "",
                Types.VARBINARY,
                "VARBINARY"
            )
        );
        Assert.assertArrayEquals(
            new byte[0],
            (byte[]) typeConvert(
                "",
                Types.LONGVARBINARY,
                "LONGVARBINARY"
            )
        );
        Assert.assertArrayEquals(
            new byte[0],
            (byte[]) typeConvert(
                "",
                Types.BIT,
                "BIT"
            )
        );
        Assert.assertArrayEquals(
            new byte[0],
            (byte[]) typeConvert(
                "",
                Types.BLOB,
                "BLOB"
            )
        );

        Assert.assertArrayEquals(
            "123".getBytes("ISO-8859-1"),
            (byte[]) typeConvert(
                "123",
                Types.BINARY,
                "BINARY"
            )
        );
        Assert.assertArrayEquals(
            "123".getBytes("ISO-8859-1"),
            (byte[]) typeConvert(
                "123",
                Types.VARBINARY,
                "VARBINARY"
            )
        );
        Assert.assertArrayEquals(
            "123".getBytes("ISO-8859-1"),
            (byte[]) typeConvert(
                "123",
                Types.LONGVARBINARY,
                "LONGVARBINARY"
            )
        );
        Assert.assertArrayEquals(
            "123".getBytes("ISO-8859-1"),
            (byte[]) typeConvert(
                "123",
                Types.BIT,
                "BIT"
            )
        );
        Assert.assertArrayEquals(
            "123".getBytes("ISO-8859-1"),
            (byte[]) typeConvert(
                "123",
                Types.BLOB,
                "BLOB"
            )
        );
    }

    private static Object typeConvert(
        String value,
        int sqlType,
        String mysqlType
    ) {
        return JdbcTypeUtil.typeTransform(
            tableName,
            columnName,
            value,
            sqlType,
            mysqlType
        );
    }

}
