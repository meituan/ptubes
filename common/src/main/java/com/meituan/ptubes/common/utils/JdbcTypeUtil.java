package com.meituan.ptubes.common.utils;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

/**
 * Type conversion tool class
 */
public class JdbcTypeUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcTypeUtil.class);

    public static Object getRSData(
        ResultSet rs,
        String columnName,
        int jdbcType
    ) throws SQLException {
        if (jdbcType == Types.BOOLEAN) {
            return rs.getByte(columnName);
        } else {
            return rs.getObject(columnName);
        }
    }

    public static Class<?> jdbcTypeToJavaType(int jdbcType) {
        switch (jdbcType) {
            case Types.BOOLEAN:
                // return Boolean.class;
            case Types.SMALLINT:
                return Short.class;
            case Types.INTEGER:
                return Integer.class;
            case Types.BIGINT:
                return Long.class;
            case Types.TINYINT:
                return Byte.TYPE;
            case Types.REAL:
                return Float.class;
            case Types.DECIMAL:
            case Types.NUMERIC:
                return BigDecimal.class;
            case Types.FLOAT:
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return String.class;
            case Types.DOUBLE:
                return Double.class;
            case Types.DATE:
                return Date.class;
            case Types.TIME:
                return Time.class;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BIT:
            case Types.BLOB:
                return byte[].class;
            case Types.TIMESTAMP:
                return Timestamp.class;
            default:
                return String.class;
        }
    }

    private static boolean isText(String columnType) {
        return "LONGTEXT".equalsIgnoreCase(columnType) || "MEDIUMTEXT".equalsIgnoreCase(columnType)
            || "TEXT".equalsIgnoreCase(columnType) || "TINYTEXT".equalsIgnoreCase(columnType);
    }

    private static boolean sqlTypeIsChar(int sqlType) {
        return sqlType == Types.CHAR || sqlType == Types.VARCHAR || sqlType == Types.LONGVARCHAR;
    }

    private static boolean sqlTypeIsBinary(int sqlType) {
        return sqlType == Types.BINARY || sqlType == Types.VARBINARY || sqlType == Types.LONGVARBINARY ||
            sqlType == Types.BIT || sqlType == Types.BLOB;
    }

    public static Object typeTransform(
        String tableName,
        String columnName,
        String value,
        int sqlType,
        String mysqlType
    ) {
        if (value == null
            || ("".equals(value) && !(isText(mysqlType) || sqlTypeIsChar(sqlType) || sqlTypeIsBinary(sqlType)))) {
            return null;
        }

        try {
            Object res;
            switch (sqlType) {
                case Types.TINYINT:
                    res = Byte.parseByte(value);
                    break;
                case Types.BIGINT:
                    if (mysqlType.startsWith("bigint") && mysqlType.endsWith("unsigned")) {
                        res = new BigInteger(value);
                    } else {
                        res = Long.parseLong(value);
                    }
                    break;
                case Types.SMALLINT:
                    res = Short.parseShort(value);
                    break;
                // case Types.BIT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                    res = new BigDecimal(value);
                    break;
                case Types.BOOLEAN:
                    res = !"0".equals(value);
                    break;
                case Types.DOUBLE:
                case Types.FLOAT:
                    res = Double.parseDouble(value);
                    break;
                case Types.REAL:
                    res = Float.parseFloat(value);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BIT:
                case Types.BLOB:
                    res = value.getBytes("ISO-8859-1");
                    break;
                case Types.DATE:
                    if (!value.startsWith("0000-00-00")) {
                        java.util.Date date = parseDate(value);
                        if (date != null) {
                            res = new Date(date.getTime());
                        } else {
                            res = null;
                        }
                    } else {
                        res = null;
                    }
                    break;
                case Types.TIME: {
                    java.util.Date date = parseDate(value);
                    if (date != null) {
                        res = new Time(date.getTime());
                    } else {
                        res = null;
                    }
                    break;
                }
                case Types.INTEGER:
                    res = Integer.parseInt(value);
                    break;
                case Types.TIMESTAMP:
                    if (!value.startsWith("0000-00-00")) {
                        java.util.Date date = parseDate(value);
                        if (date != null) {
                            res = new Timestamp(date.getTime());
                        } else {
                            res = null;
                        }
                    } else {
                        res = null;
                    }
                    break;
                case Types.CLOB:
                default:
                    res = value;
                    break;
            }
            return res;
        } catch (Exception e) {
            LOG.error(String.format(
                "table: %s column: %s, failed convert type %s to %s.",
                tableName,
                columnName,
                value,
                sqlType
            ));
            return value;
        }
    }

    public static java.util.Date parseDate(String datetimeStr) {
        if (StringUtils.isEmpty(datetimeStr)) {
            return null;
        }
        datetimeStr = datetimeStr.trim();
        if (datetimeStr.contains("-")) {
            if (datetimeStr.contains(":")) {
                datetimeStr = datetimeStr.replace(
                    " ",
                    "T"
                );
            }
        } else if (datetimeStr.contains(":")) {
            datetimeStr = "T" + datetimeStr;
        }

        DateTime dateTime = new DateTime(datetimeStr);

        return dateTime.toDate();
    }
}
