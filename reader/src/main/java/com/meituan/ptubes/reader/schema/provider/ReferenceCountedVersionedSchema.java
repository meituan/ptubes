package com.meituan.ptubes.reader.schema.provider;

import com.meituan.ptubes.common.exception.IllegalRefCntException;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;

public class ReferenceCountedVersionedSchema {

	private final WriteTaskSchemaProvider parent;
	private	final VersionedSchema versionedSchema;
	
	private volatile int refCnt;

	public ReferenceCountedVersionedSchema(WriteTaskSchemaProvider parent, VersionedSchema versionedSchema) {
		this.refCnt = 1;
		this.parent = parent;
		this.versionedSchema = versionedSchema;
	}

	public VersionedSchema getVersionedSchema() {
		return versionedSchema;
	}

	public int refCnt() {
		return this.refCnt;
	}

	private void checkPositive(int increment) throws IllegalArgumentException {
		if (increment <= 0) {
			throw new IllegalArgumentException("illegal increment for refCnt = " + increment);
		}
	}

	private void checkNegative(int decrement) throws IllegalArgumentException {
		if (decrement >= 0) {
			throw new IllegalArgumentException("illegal decrement for refCnt = " + decrement);
		}
	}

	private void checkRelease() throws IllegalRefCntException {
		if (this.refCnt <= 0) {
			throw new IllegalRefCntException(versionedSchema.getVersion().getTableFullName() + " ver " + versionedSchema.getVersion().getVersion()
					+ " refCnt = 0, have been released already");
		}
	}

	private void checkOverflow(int increment) throws IllegalRefCntException {
		if (this.refCnt >= 0 && this.refCnt + increment < this.refCnt) {
			throw new IllegalRefCntException("refCnt = " + refCnt + ", increment = " + increment + " make refCnt overflow");
		}
	}

	private void checkPassZero(int decrement) throws IllegalRefCntException {
		if (this.refCnt >= 0 && this.refCnt + decrement < 0) {
			throw new IllegalRefCntException("refCnt = " + refCnt + ", decrement = " + decrement + " make refCnt pass zero");
		}
	}

	/**
	 * Requires external lock support, different from ReferenceCountedByteBuf design
	 * @param increment
	 * @return
	 * @throws IllegalRefCntException
	 */
	@Deprecated
	/** Assumes there is already a write lock */
	public ReferenceCountedVersionedSchema refCntRetain(int increment) {
		checkPositive(increment);
		checkRelease();
		checkOverflow(increment);

		this.refCnt += increment;
		return this;
	}

	/** Assumes there is already a write lock */
	public ReferenceCountedVersionedSchema refCntRetain() {
		checkRelease();
		checkOverflow(1);

		this.refCnt++;
		return this;
	}

	/** Assumes there is already a write lock */
	public boolean refCntRelease(int decrement) throws IllegalRefCntException {
		checkNegative(decrement);
		checkRelease();
		checkPassZero(decrement);

		this.refCnt -= decrement;
		if (this.refCnt == 0) {
			return true;
		} else {
			return false;
		}
	}

	/** Assumes there is already a write lock */
	public boolean refCntRelease() throws IllegalRefCntException {
		checkRelease();

		this.refCnt--;
		if (this.refCnt == 0) {
			return true;
		} else {
			return false;
		}
	}

	public boolean release() throws InterruptedException {
		return parent.releaseSchema(this);
	}

}
