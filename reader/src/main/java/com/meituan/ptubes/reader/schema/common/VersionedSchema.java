package com.meituan.ptubes.reader.schema.common;

public class VersionedSchema {

	private final SchemaVersion version;
	private final String tableMetaContent;
	private final TableMeta tableMeta;

	public VersionedSchema(String tableMetaContent, TableMeta tableMeta, SchemaVersion version) {
		this.tableMetaContent = tableMetaContent;
		this.tableMeta = tableMeta;
		this.version = version;
	}

	public SchemaVersion getVersion() {
		return version;
	}

	public TableMeta getTableMeta() {
		return tableMeta;
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();

		res.append("[ schemaVersion=").append(version.toString())
			.append(", schema=").append(this.tableMetaContent)
            .append(", partKeys=").append(tableMeta.getPkFieldListStr())
            .append(", priKeys=").append(tableMeta.getPrimaryKeyFieldListStr()).append(" ]");

		return res.toString();
	}
}
