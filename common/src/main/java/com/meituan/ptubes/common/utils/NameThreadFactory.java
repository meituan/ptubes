package com.meituan.ptubes.common.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


public class NameThreadFactory implements ThreadFactory {
	private static AtomicInteger POOL_ID = new AtomicInteger(0);

	private AtomicInteger threadId = new AtomicInteger(0);

	private String prefix;

	private boolean daemon;

	public NameThreadFactory(String prefix, boolean daemon) {
		this.prefix = prefix + "-Pool-" + POOL_ID.incrementAndGet() + "-Thread-";
		this.daemon = daemon;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread thread = new Thread(r);
		thread.setName(prefix + threadId.incrementAndGet());
		thread.setDaemon(daemon);
		return thread;
	}
}
