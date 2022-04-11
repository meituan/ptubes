package com.meituan.ptubes.reader.container.common;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractLifeCycle implements LifeCycle {
	/** Is it running **/
	protected final AtomicBoolean running = new AtomicBoolean(false);

	@Override
	public boolean isStart() {
		return running.get();
	}

	@Override
	public boolean start() {
		return running.compareAndSet(false, true);
	}

	@Override
	public boolean stop() {
		return running.compareAndSet(true, false);
	}

}
