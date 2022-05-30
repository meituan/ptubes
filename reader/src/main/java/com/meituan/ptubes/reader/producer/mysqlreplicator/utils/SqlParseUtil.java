package com.meituan.ptubes.reader.producer.mysqlreplicator.utils;

import org.apache.commons.lang.StringUtils;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlParseUtil {
	private static final Logger LOG = LoggerFactory.getLogger(SqlParseUtil.class);
	private static final Character COMMA_SEPARATOR = ',';
	public enum QUERY_EVENT_TYPE {
		BEGIN,
		RENAME,
		ALTER,
		CREATE,
		UNKNOWN
	}

	private final static Pattern CREATE_SQL_TABLENAME_PATTERN = Pattern.compile(
			"^\\s*create\\s*table\\s+(?:if\\s+not\\s+exists\\s+)?(?<tableName>(?:(?![\\s+(]).)*)", Pattern.CASE_INSENSITIVE);

	private final static Pattern COMMENT_PATTERN = Pattern.compile("^\\s*/\\*(?<comment>.*?)\\*/");

	public static String getSqlFromEvent(String sqlFromEvent) {
		return sqlFromEvent.replaceAll("[\r\n\t]", " ").replaceAll("/\\*.*?\\*/", "").replaceAll(" +", " ").trim();
	}

	public static QUERY_EVENT_TYPE getQueryEventType(String sql) {
		if ("BEGIN".equalsIgnoreCase(sql)) {
			return QUERY_EVENT_TYPE.BEGIN;
		} else {
			String sqlType = sql.split("\\s+")[0].trim();
			if ("RENAME".equalsIgnoreCase(sqlType)) {
				return QUERY_EVENT_TYPE.RENAME;
			} else if ("ALTER".equalsIgnoreCase(sqlType)) {
				return QUERY_EVENT_TYPE.ALTER;
			} else if ("CREATE".equalsIgnoreCase(sqlType)) {
				return QUERY_EVENT_TYPE.CREATE;
			} else {
				return QUERY_EVENT_TYPE.UNKNOWN;
			}
		}
	}

	public static String parseDbTableNameFromCreateSql(String database, String sql) {
		LOG.info("Create sql: " + sql);
		Matcher m = CREATE_SQL_TABLENAME_PATTERN.matcher(sql);
		if (!m.find()) {
			return null;
		}
		String tableName = m.group("tableName");

		if (StringUtils.isBlank(database)) {
			database = getDatabase(tableName);
		}

		tableName = getRealTable(tableName);

		return database + "." + tableName;
	}

	public static String parseDbTableNameFromAlterTableSql(String database, String sql) {
		// eg. "ALTER TABLE test_ln.test_table_2 DROP Birthday" "ALTER TABLE test_table CHANGE column int_t INT_T int"
		LOG.info("Alter sql: " + sql);
		String[] splitResult = sql.split("\\s+");
		String tableName = null;
		if (splitResult.length > 3 && "RENAME".equalsIgnoreCase(splitResult[3])) {
			// case: "ALTER TABLE table_name1 rename to table_name2"
			tableName = splitResult[splitResult.length - 1];
		} else {
			// case: "ALTER TABLE test_ln.test_table_2 DROP Birthday", "ALTER TABLE test_table CHANGE column int_t INT_T int", "ALTER TABLE test_table"
			tableName = splitResult[2];
		}

		if (StringUtils.isBlank(database)) {
			database = getDatabase(tableName);
		}

		tableName = getRealTable(tableName);

		return database + "." + tableName;
	}

	public static String parseDbTableNameFromRenameTableSql(String database, String sql) {
		LOG.info("Rename sql: " + sql);
		String[] splitResult = sql.split("\\s+");
		String tableName = splitResult[splitResult.length - 1];

		if (StringUtils.isBlank(database)) {
			database = getDatabase(tableName);
		}

		tableName = getRealTable(tableName);

		return database + "." + tableName;
	}

	private static String getDatabase(String tableName) {
		String tmp = tableName.replace("`", "").replace(";", "").trim();
		if (tmp.contains(".")) {
			String[] tableNames = tmp.split("\\.");
			return tableNames[0].trim();
		} else {
			return "";
		}
	}

	private static String getRealTable(String tableName) {
		String tmp = tableName.replace("`", "").replace(";", "").trim();
		if (tmp.contains(".")) {
			String[] tableNames = tmp.split("\\.");
			tmp = tableNames[tableNames.length - 1];
		}
		return tmp;
	}

	/**
	 * DML format
	 * see https://dev.mysql.com/doc/refman/5.7/en/sql-data-manipulation-statements.html DML related formats in sql-statement
	 */
	enum CommentState {
		SEARCHING_START_SLASH,
		SEARCHING_START_STAR,
		SEARCHING_END_STAR,
		SEARCHING_END_SLASH;
	}
	public static boolean startsWithIgnoreCase(String origin, String prefix, int offset) {
		if (offset + prefix.length() < origin.length()) {
			String subString = origin.substring(offset, offset + prefix.length());
			return StringUtils.equalsIgnoreCase(subString, prefix);
		} else {
			return false;
		}
	}
	public static String getCommentV2(String originalSql) {
		if (StringUtils.isBlank(originalSql)) {
			return "";
		}

		StringBuilder res = new StringBuilder();

		boolean breakLoop = false;
		char[] originalCharArray = originalSql.toCharArray();
		int commentStartIndex = 0;
		int commentEndIndex = 0;
		CommentState state = CommentState.SEARCHING_START_SLASH;
		int index;
		for (index = 0; index < originalCharArray.length; ++index) {
			char c = originalCharArray[index];
			switch (state) {
				case SEARCHING_START_SLASH:
					if (c == '/') {
						state = CommentState.SEARCHING_START_STAR;
					} else if (c == ' ' || c == '\t') {
						// keep searching
					} else {
						if (startsWithIgnoreCase(originalSql, "insert", index) ||
							startsWithIgnoreCase(originalSql, "update", index) ||
							startsWithIgnoreCase(originalSql, "delete", index) ||
							startsWithIgnoreCase(originalSql, "replace", index)) {
							breakLoop = true;
						} else {
							// keep searching
						}
					}
					break;
				case SEARCHING_START_STAR:
					if (c == '*') {
						state = CommentState.SEARCHING_END_STAR;
						commentStartIndex = index + 1;
					} else {
						state = CommentState.SEARCHING_START_STAR;
					}
					break;
				case SEARCHING_END_STAR:
					if (c == '*') {
						state = CommentState.SEARCHING_END_SLASH;
						commentEndIndex = index;
					}
					break;
				case SEARCHING_END_SLASH:
					if (c == '/') {
						state = CommentState.SEARCHING_START_SLASH;
						// output
						String subRes = originalSql.substring(commentStartIndex, commentEndIndex).trim();
						if (StringUtils.isNotBlank(subRes)) {
							if (res.length() > 0) {
								res.append(COMMA_SEPARATOR);
							}
							res.append(subRes);
						}
					} else if (c == '*') {
						state = CommentState.SEARCHING_END_SLASH;
						commentEndIndex = index;
					} else {
						state = CommentState.SEARCHING_END_STAR;
					}
					break;
				default:
					breakLoop = true;
					break;
			}

			if (breakLoop) {
				break;
			}
		}

		breakLoop = false;
		int boundaryIndex = index;
		state = CommentState.SEARCHING_END_SLASH;
		for (index = originalCharArray.length - 1; index >= boundaryIndex; --index) {
			char c = originalCharArray[index];
			switch (state) {
				case SEARCHING_END_SLASH:
					if (c == '/') {
						state = CommentState.SEARCHING_END_STAR;
					} else if (c == ' ' || c == '\t') {
						// keep searching
					} else {
						breakLoop = true;
					}
					break;
				case SEARCHING_END_STAR:
					if (c == '*') {
						state = CommentState.SEARCHING_START_STAR;
						commentEndIndex = index;
					} else {
						breakLoop = true;
					}
					break;
				case SEARCHING_START_STAR:
					if (c == '*') {
						state = CommentState.SEARCHING_START_SLASH;
						commentStartIndex = index + 1;
					} else {
						// keep searching
					}
					break;
				case SEARCHING_START_SLASH:
					if (c == '/') {
						String subRes = originalSql.substring(commentStartIndex, commentEndIndex).trim();
						if (StringUtils.isNotBlank(subRes)) {
							if (res.length() > 0) {
								res.append(COMMA_SEPARATOR);
							}
							res.append(subRes);
						}
						breakLoop = true;
					} else if (c == '*') {
						state = CommentState.SEARCHING_START_SLASH;
						commentStartIndex = index + 1;
					} else {
						state = CommentState.SEARCHING_START_STAR;
					}
					break;
				default:
					breakLoop = true;
					break;
			}

			if (breakLoop) {
				break;
			}
		}

		return res.toString();
	}
}
