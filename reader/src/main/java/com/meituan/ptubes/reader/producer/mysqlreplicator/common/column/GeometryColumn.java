package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

import com.vividsolutions.jts.geom.Geometry;

public class GeometryColumn implements Column {
	private Geometry geometry;

	public GeometryColumn(Geometry g) {
		this.geometry = g;
	}

	@Override public Geometry getValue() {
		return this.geometry;
	}

	public static GeometryColumn valueOf(Geometry g) {
		return new GeometryColumn(g);
	}
}

