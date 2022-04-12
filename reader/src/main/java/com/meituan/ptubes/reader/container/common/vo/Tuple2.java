package com.meituan.ptubes.reader.container.common.vo;

/**
 * Two-tuples, similar to guava, scala two-tuples, can inherit this object to construct triples, quadruples, etc.
 * @param <D1>
 * @param <D2>
 */
public class Tuple2<D1, D2> {

	private D1 d1;
	private D2 d2;

	public Tuple2(D1 d1, D2 d2) {
		this.d1 = d1;
		this.d2 = d2;
	}

	public D1 getD1() {
		return d1;
	}

	public void setD1(D1 d1) {
		this.d1 = d1;
	}

	public D2 getD2() {
		return d2;
	}

	public void setD2(D2 d2) {
		this.d2 = d2;
	}
}
