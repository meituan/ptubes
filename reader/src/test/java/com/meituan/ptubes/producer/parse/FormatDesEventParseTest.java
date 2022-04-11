package com.meituan.ptubes.producer.parse;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.FormatDescriptionEvent;
import org.junit.Assert;
import org.junit.Test;

public class FormatDesEventParseTest {

    @Test
    public void testVersionParse() throws Exception {
        FormatDescriptionEvent fde = new FormatDescriptionEvent();

        fde.setServerVersion(StringColumn.valueOf("5.7.25-28.mtsql.1.0.0-log".getBytes()));
        Assert.assertTrue(fde.checksumPossible());

        fde.setServerVersion(StringColumn.valueOf("5.7.25-28-mtsql-1-0-0-log".getBytes()));
        Assert.assertTrue(fde.checksumPossible());

        fde.setServerVersion(StringColumn.valueOf("5.7.26-29-log".getBytes()));
        Assert.assertTrue(fde.checksumPossible());
    }
}
