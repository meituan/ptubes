package com.meituan.ptubes.reader.schema.util;

import com.meituan.ptubes.reader.container.common.vo.PtubesFieldType;

// dbType -> javaType
public enum SchemaPrimitiveTypes {
	// numberic data type
	TINYINT("int", PtubesFieldType.INT),
	SMALLINT("int", PtubesFieldType.INT),
	MEDIUMINT("int", PtubesFieldType.INT),
	INTEGER("int", PtubesFieldType.INT),
	INT("long", PtubesFieldType.LONG),
	LONG("long", PtubesFieldType.LONG),
	BIGINT("long", PtubesFieldType.LONG),
	DECIMAL("string", PtubesFieldType.STRING),

	TINYINT_UNSIGNED("int", PtubesFieldType.INT),
	SMALLINT_UNSIGNED("int", PtubesFieldType.INT),
	MEDIUMINT_UNSIGNED("int", PtubesFieldType.INT),
	INTEGER_UNSIGNED("int", PtubesFieldType.LONG),
	INT_UNSIGNED("long", PtubesFieldType.LONG),
	BIGINT_UNSIGNED("long", PtubesFieldType.LONG),
	DECIMAL_UNSIGNED("string", PtubesFieldType.STRING),

	FLOAT("float", PtubesFieldType.FLOAT),
	DOUBLE("double", PtubesFieldType.DOUBLE),

	// string data type
	VARCHAR("string", PtubesFieldType.STRING),
	VARCHAR2("string", PtubesFieldType.STRING),
	NVARCHAR("string", PtubesFieldType.STRING),
	NVARCHAR2("string", PtubesFieldType.STRING),
	CHAR("string", PtubesFieldType.STRING),
	TINYTEXT("string", PtubesFieldType.STRING),
	TEXT("string", PtubesFieldType.STRING),
	MEDIUMTEXT("string", PtubesFieldType.STRING),
	LONGTEXT("string", PtubesFieldType.STRING),
	CLOB("string", PtubesFieldType.STRING),

	BIT("bytes", PtubesFieldType.BYTES),
	TINYBLOB("bytes", PtubesFieldType.BYTES),
	MEDIUMBLOB("bytes", PtubesFieldType.BYTES),
	BLOB("bytes", PtubesFieldType.BYTES),
	LONGBLOB("bytes", PtubesFieldType.BYTES),
	BINARY("string", PtubesFieldType.BYTES),
	VARBINARY("string", PtubesFieldType.BYTES),

	// its better to use long java-type
	ENUM("string", PtubesFieldType.STRING),
	SET("string", PtubesFieldType.STRING),

	// time data type
	YEAR("int", PtubesFieldType.INT),
	DATE("long", PtubesFieldType.DATE),
	TIME("long", PtubesFieldType.TIME),
	TIMESTAMP("long", PtubesFieldType.TIMESTAMP),
	DATETIME("long", PtubesFieldType.DATETIME),

	// json data type
	JSON("string", PtubesFieldType.STRING),

	// spatial data type
	GEOMETRY("string", PtubesFieldType.GEOMETRY),
	POINT("string", PtubesFieldType.GEOMETRY),
	LINESTRING("string", PtubesFieldType.GEOMETRY),
	POLYGON("string", PtubesFieldType.GEOMETRY),
	MULTIPOINT("string", PtubesFieldType.GEOMETRY),
	MULTILINESTRING("string", PtubesFieldType.GEOMETRY),
	MULTIPOLYGON("string", PtubesFieldType.GEOMETRY),
	GEOMETRYCOLLECTION("string", PtubesFieldType.GEOMETRY),

	// others
	RAW("bytes", PtubesFieldType.BYTES),
	ARRAY("array", PtubesFieldType.OTHERS),
	TABLE("record", PtubesFieldType.OTHERS),
	XMLTYPE("string", PtubesFieldType.STRING);

	private final String schemaType;
	private final PtubesFieldType ptubesFieldType;

	private SchemaPrimitiveTypes(String schemaType, PtubesFieldType ptubesFieldType) {
		this.schemaType = schemaType;
		this.ptubesFieldType = ptubesFieldType;
	}

	public String getSchemaType() {
		return schemaType;
	}

	public PtubesFieldType getPtubesFieldType() {
		return ptubesFieldType;
	}

	public static boolean isBytesType(String fieldType) {
		if (BIT.name()
			.equalsIgnoreCase(fieldType) || RAW.name()
			.equalsIgnoreCase(fieldType) || BLOB.name()
			.equalsIgnoreCase(fieldType) || TINYBLOB.name()
			.equalsIgnoreCase(fieldType) || MEDIUMBLOB.name()
			.equalsIgnoreCase(fieldType) || LONGBLOB.name()
			.equalsIgnoreCase(fieldType) || BINARY.name()
			.equalsIgnoreCase(fieldType) || VARBINARY.name()
			.equalsIgnoreCase(fieldType)) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isSignSensitive(String fieldType) {
		if (TINYINT.name().equalsIgnoreCase(fieldType) ||
			SMALLINT.name().equalsIgnoreCase(fieldType) ||
			MEDIUMINT.name().equalsIgnoreCase(fieldType) ||
			INTEGER.name().equalsIgnoreCase(fieldType) ||
			INT.name().equalsIgnoreCase(fieldType) ||
			BIGINT.name().equalsIgnoreCase(fieldType) ||
			DECIMAL.name().equalsIgnoreCase(fieldType)) {
			return true;
		}
		return false;
	}

	public static boolean isSpatialType(String fieldType) {
		if (GEOMETRY.name().equalsIgnoreCase(fieldType) ||
			POINT.name().equalsIgnoreCase(fieldType) ||
			LINESTRING.name().equalsIgnoreCase(fieldType) ||
			POLYGON.name().equalsIgnoreCase(fieldType) ||
			MULTIPOINT.name().equalsIgnoreCase(fieldType) ||
			MULTILINESTRING.name().equalsIgnoreCase(fieldType) ||
			MULTIPOLYGON.name().equalsIgnoreCase(fieldType) ||
			GEOMETRYCOLLECTION.name().equalsIgnoreCase(fieldType)) {
			return true;
		}
		return false;
	}
}
