package com.meituan.ptubes.producer.parse;

import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.SqlParseUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class SqlParseTest {

    private static final Logger LOG = LoggerFactory.getLogger(SqlParseTest.class);

    @Test
    public void getCommentV2Test() {
        List<Pair<String, String>> tcs = new ArrayList<>();
        tcs.add(Pair.of("update sql", ""));
        tcs.add(Pair.of("/* ptubes.cell=setA */ update sql", "ptubes.cell=setA"));
        tcs.add(Pair.of("/*ptubes.cell=setA */ update sql", "ptubes.cell=setA"));
        tcs.add(Pair.of("/*ptubes.cell=setA */ update sql /* ptubes.cell=setB */", "ptubes.cell=setA,ptubes.cell=setB"));
        tcs.add(Pair.of("/*ptubes.cell= */ update sql /* ptubes.cell=setB */", "ptubes.cell=,ptubes.cell=setB"));
        tcs.add(Pair.of("/*ptubes.cell=setA * update sql /* ptubes.cell=setB */", "ptubes.cell=setA * update sql /* ptubes.cell=setB"));
        tcs.add(Pair.of("/*= */ update sql /* ptubes.cell=setB */", "=,ptubes.cell=setB"));
        tcs.add(Pair.of("/* */ update sql /* ptubes.cell=setB */", "ptubes.cell=setB"));
        tcs.add(Pair.of("/** update sql /* ptubes.cell=setB */", "* update sql /* ptubes.cell=setB"));
        tcs.add(Pair.of("update sql /* ptubes.cell=setB */", "ptubes.cell=setB"));
        tcs.add(Pair.of("update sql /*/ ptubes.cell=setB", ""));
        tcs.add(Pair.of("update sql ptubes.cell=setB */", ""));
        tcs.add(Pair.of("update sql /* ptubes.cell=setB", ""));
        tcs.add(Pair.of("", ""));
        tcs.add(Pair.of(" ", ""));
        tcs.add(Pair.of(null, ""));
        tcs.add(Pair.of("UpDate sql", ""));
        tcs.add(Pair.of("dfafd sql", ""));
        tcs.add(Pair.of("fdsafdsafeafdasfe", ""));
//        tcs.add(Pair.of("/* fdsaf insert */ update sql", "fdsaf insert"));
        tcs.add(Pair.of("/* abc=abc */ sql /* cba=cba */", "abc=abc,cba=cba"));
        tcs.add(Pair.of("/* abc=abc */ UpdaTe sql /* cba=cba */", "abc=abc,cba=cba"));
        tcs.add(Pair.of("/* *abc=abc **/ UpdaTe sql /**cba=cba ***/", "*abc=abc *,*cba=cba **"));
        tcs.add(Pair.of("/**/", ""));
        tcs.add(Pair.of("/*** *** ***/", "** *** **"));
        tcs.add(Pair.of("/*/", ""));
        tcs.add(Pair.of("/* comment1 */ /*** comment2 ***/ /***comment3(321)***/ sql /**tailer comment */", "comment1,** comment2 **,**comment3(321)**,*tailer comment"));
        tcs.add(Pair.of("/* comment1 */ /*** comment2 ***/ /***comment3(321)***/ /**tailer comment */", "comment1,** comment2 **,**comment3(321)**,*tailer comment"));
        tcs.add(Pair.of("/* comment1 *//*** comment2 ***//***comment3(321)***/sql;/**tailer comment */", "comment1,** comment2 **,**comment3(321)**,*tailer comment"));
        tcs.add(Pair.of("/* updatecomment1 *//***UPDATE comment2 ***//***comment3(321)***/uPDate sql/**tailer comment */;", "updatecomment1,**UPDATE comment2 **,**comment3(321)**"));

        for (Pair<String, String> tc : tcs) {
            String originSql = tc.getKey();
            String expected = tc.getValue();
            String result;
            try {
                result = SqlParseUtil.getCommentV2(originSql);
                LOG.info("origin: {}, expected: {}, result: {}", originSql, expected, result);
            } catch (Throwable te) {
                LOG.error("origin: {}, expected: {}, result: {}", originSql, expected, "parse error!!");
                throw te;
            }
            Assert.assertEquals(expected, result);
        }
    }

    @Test
    public void getSqlFromEventTest() {
        {
            String sql = "/* ptubes.cell=setA */ update sql";
            Assert.assertEquals(
                "update sql",
                SqlParseUtil.getSqlFromEvent(sql)
            );
        }

        {
            String sql = "/*ptubes.cell=setA */ update sql";
            Assert.assertEquals(
                "update sql",
                SqlParseUtil.getSqlFromEvent(sql)
            );
        }

        {
            String sql = "/*ptubes.cell=setA */ update sql /* ptubes.cell=setB */";
            Assert.assertEquals(
                "update sql",
                SqlParseUtil.getSqlFromEvent(sql)
            );
        }

        {
            String sql = "/*ptubes.cell= */ update sql /* ptubes.cell=setB */";
            Assert.assertEquals(
                "update sql",
                SqlParseUtil.getSqlFromEvent(sql)
            );
        }

        {
            String sql = "/*ptubes.cell=setA * update sql /* ptubes.cell=setB */";
            Assert.assertEquals(
                "",
                SqlParseUtil.getSqlFromEvent(sql)
            );
        }

        {
            String sql = "/*= */ update sql /* ptubes.cell=setB */";
            Assert.assertEquals(
                "update sql",
                SqlParseUtil.getSqlFromEvent(sql)
            );
        }

        {
            String sql = "/* */ update sql /* ptubes.cell=setB */";
            Assert.assertEquals(
                "update sql",
                SqlParseUtil.getSqlFromEvent(sql)
            );
        }

        {
            String sql = "/** update sql /* ptubes.cell=setB */";
            Assert.assertEquals(
                "",
                SqlParseUtil.getSqlFromEvent(sql)
            );
        }

        {
            String sql = "update sql /* ptubes.cell=setB */";
            Assert.assertEquals(
                "update sql",
                SqlParseUtil.getSqlFromEvent(sql)
            );
        }
    }

    @Test
    public void parseDbTableNameFromSqlTest() {
        // alter
        {
            String sql = "alter table table_a change  column operator_time operate_time  timestamp default CURRENT_TIMESTAMP not null comment 'Operation time'";
            Assert.assertEquals(
                SqlParseUtil.parseDbTableNameFromAlterTableSql(
                    "db",
                    sql
                ),
                "db.table_a"
            );
        }

        {
            String sql = "alter table db.table_a change  column operator_time operate_time  timestamp default CURRENT_TIMESTAMP not null comment 'Operation time'";
            Assert.assertEquals(
                SqlParseUtil.parseDbTableNameFromAlterTableSql(
                    "db",
                    sql
                ),
                "db.table_a"
            );
        }

        {
            String sql = "alter table db_a.table_a change  column operator_time operate_time  timestamp default CURRENT_TIMESTAMP not null comment 'Operation time'";
            Assert.assertEquals(
                SqlParseUtil.parseDbTableNameFromAlterTableSql(
                    "db",
                    sql
                ),
                "db.table_a"
            );
        }

        // rename
        {
            String sql = "rename table table_a to table_b";
            Assert.assertEquals(
                SqlParseUtil.parseDbTableNameFromRenameTableSql(
                    "db",
                    sql
                ),
                "db.table_b"
            );
        }

        {
            String sql = "rename table db.table_a to db.table_b";
            Assert.assertEquals(
                SqlParseUtil.parseDbTableNameFromRenameTableSql(
                    "db",
                    sql
                ),
                "db.table_b"
            );
        }

        {
            String sql = "rename table db_a.table_a to db_a.table_b";
            Assert.assertEquals(
                SqlParseUtil.parseDbTableNameFromRenameTableSql(
                    "db",
                    sql
                ),
                "db.table_b"
            );
        }

        // create
        {
            String sql = "create table `table_a` (`id` bigint(20) NOT NULL AUTO_INCREMENT";
            Assert.assertEquals(
                SqlParseUtil.parseDbTableNameFromCreateSql(
                    "db",
                    sql
                ),
                "db.table_a"
            );
        }

        {
            String sql = "create table `db.table_a` (`id` bigint(20) NOT NULL AUTO_INCREMENT";
            Assert.assertEquals(
                SqlParseUtil.parseDbTableNameFromCreateSql(
                    "db",
                    sql
                ),
                "db.table_a"
            );
        }

        {
            String sql = "create table `db_a.table_a` (`id` bigint(20) NOT NULL AUTO_INCREMENT";
            Assert.assertEquals(
                SqlParseUtil.parseDbTableNameFromCreateSql(
                    "db",
                    sql
                ),
                "db.table_a"
            );
        }

    }

    @Test
    public void getQueryEventTypeTest() throws PtubesException {
        {
            String sql = "alter table table_a change  column operator_time operate_time  timestamp default CURRENT_TIMESTAMP not null comment 'Operation time'";
            Assert.assertEquals(
                SqlParseUtil.QUERY_EVENT_TYPE.ALTER,
                SqlParseUtil.getQueryEventType(sql)
            );
        }
        {
            String sql = "ALTER table table_a change  column operator_time operate_time  timestamp default CURRENT_TIMESTAMP not null comment 'Operation time'";
            Assert.assertEquals(
                SqlParseUtil.QUERY_EVENT_TYPE.ALTER,
                SqlParseUtil.getQueryEventType(sql)
            );
        }
        {
            String sql = "rename table table_a to table_b, table_c to table_d";
            Assert.assertEquals(
                SqlParseUtil.QUERY_EVENT_TYPE.RENAME,
                SqlParseUtil.getQueryEventType(sql)
            );
        }
        {
            String sql = "RENAME table table_a to table_b, table_c to table_d";
            Assert.assertEquals(
                SqlParseUtil.QUERY_EVENT_TYPE.RENAME,
                SqlParseUtil.getQueryEventType(sql)
            );
        }
        {
            String sql = "create table `table_a` (`id` bigint(20) NOT NULL AUTO_INCREMENT";
            Assert.assertEquals(
                SqlParseUtil.QUERY_EVENT_TYPE.CREATE,
                SqlParseUtil.getQueryEventType(sql)
            );
        }
        {
            String sql = "CREATE table `table_a` (`id` bigint(20) NOT NULL AUTO_INCREMENT";
            Assert.assertEquals(
                SqlParseUtil.QUERY_EVENT_TYPE.CREATE,
                SqlParseUtil.getQueryEventType(sql)
            );
        }
        {
            String sql = "begin";
            Assert.assertEquals(
                SqlParseUtil.QUERY_EVENT_TYPE.BEGIN,
                SqlParseUtil.getQueryEventType(sql)
            );
        }
        {
            String sql = "BEGIN";
            Assert.assertEquals(
                SqlParseUtil.QUERY_EVENT_TYPE.BEGIN,
                SqlParseUtil.getQueryEventType(sql)
            );
        }

    }
}
