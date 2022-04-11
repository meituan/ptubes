package com.meituan.ptubes.reader.schema.util;

import com.meituan.ptubes.common.exception.InvalidConfigException;


public class SchemaHelper {

	public SchemaHelper() {
	}

	public enum SchemaType {
		insert(0), update(1), delete(2), ddl(3), upsert(-1);

		private int code;

		SchemaType(int code) {
			this.code = code;
		}

		public static SchemaHelper.SchemaType fetchByCode(int code) throws InvalidConfigException {
			SchemaHelper.SchemaType[] var1 = values();
			int var2 = var1.length;

			for (int var3 = 0; var3 < var2; ++var3) {
				SchemaHelper.SchemaType t = var1[var3];
				if (t.getCode() == code) {
					return t;
				}
			}

			throw new InvalidConfigException("Invalid schema type, code: " + code);
		}

		public int getCode() {
			return this.code;
		}

		public String getUpperName() {
			return this.name().toUpperCase();
		}

		public boolean needDiffMap() {
			return this.equals(update);
		}
	}
}
