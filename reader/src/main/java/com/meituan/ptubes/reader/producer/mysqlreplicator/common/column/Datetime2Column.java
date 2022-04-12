/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

import java.sql.Timestamp;

public final class Datetime2Column extends AbstractDatetimeColumn {

	private static final long serialVersionUID = 6444968242222031354L;

	private final java.util.Date value;
	private final String stringValue;

	private Datetime2Column(Timestamp value, String stringValue) {
		this.timestampValue = value;
		this.value = (java.util.Date) value;
		this.stringValue = stringValue;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	@Override public java.util.Date getValue() {
		return this.value;
	}

	@Override public String getStringValue() {
		return stringValue;
	}

	public static final Datetime2Column valueOf(Timestamp value, String stringValue) {
		return new Datetime2Column(value, stringValue);
	}
}
