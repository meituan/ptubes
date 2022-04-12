package com.meituan.ptubes.reader.schema.common;

import java.util.Comparator;

/**
 *  <source>.<schema>.<table>.<version>.avsc
 * | source | database+table | version |
 */

public class SchemaVersion {

	private final int version; /* version >= 1 */
	private final long eventTime;
	/*private final String source;*/
	private final String tableFullName;

	public SchemaVersion(String tableFullName, int version) {
		this.tableFullName = tableFullName;
		this.version = version;
		this.eventTime = -1;
	}

	public SchemaVersion(String tableFullName, int version, long eventTime) {
		this.tableFullName = tableFullName;
		this.version = version;
		this.eventTime = eventTime;
	}

	public int getVersion() {
		return version;
	}

	public String getTableFullName() {
		return tableFullName;
	}

	public long getEventTime() {
		return eventTime;
	}

	@Override
	public boolean equals(Object o) {
		if (null == o || !(o instanceof SchemaVersion)) {
			return false;
		}
		SchemaVersion other = (SchemaVersion) o;
		return version == other.version && tableFullName.equals(other.tableFullName);
	}

	@Override
	public int hashCode() {
		return tableFullName.hashCode() ^ version;
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();
		res.append("[ table=").append(tableFullName);
		res.append(", version=").append(version).append(" ]");

		return res.toString();
	}

	public static final class SchemaVersionComparator implements Comparator<SchemaVersion> {
		@Override public int compare(SchemaVersion o1, SchemaVersion o2) {
			return o1.getVersion() - o2.getVersion();
		}
	}
}
