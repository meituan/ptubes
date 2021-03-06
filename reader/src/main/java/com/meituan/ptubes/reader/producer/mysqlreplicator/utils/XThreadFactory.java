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
package com.meituan.ptubes.reader.producer.mysqlreplicator.utils;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class XThreadFactory implements ThreadFactory {
	//
	private static final Logger LOG = LoggerFactory.getLogger(XThreadFactory.class);

	//
	protected String name;
	protected final AtomicBoolean daemon;
	protected final AtomicBoolean trackThreads;
	protected final List<WeakReference<Thread>> threads;
	protected final ConcurrentHashMap<String, AtomicLong> sequences;
	protected final AtomicReference<UncaughtExceptionHandler> uncaughtExceptionHandler;

	public XThreadFactory() {
		this(null, false, null);
	}

	public XThreadFactory(String name) {
		this(name, false, null);
	}

	public XThreadFactory(String name, boolean daemon) {
		this(name, daemon, null);
	}

	public XThreadFactory(String name, boolean daemon, UncaughtExceptionHandler handler) {
		this.name = name;
		this.daemon = new AtomicBoolean(daemon);
		this.trackThreads = new AtomicBoolean(false);
		this.threads = new LinkedList<WeakReference<Thread>>();
		this.sequences = new ConcurrentHashMap<String, AtomicLong>();
		this.uncaughtExceptionHandler = new AtomicReference<UncaughtExceptionHandler>(handler);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isDaemon() {
		return daemon.get();
	}

	public void setDaemon(boolean daemon) {
		this.daemon.set(daemon);
	}

	public UncaughtExceptionHandler getUncaughtExceptionHandler() {
		return uncaughtExceptionHandler.get();
	}

	public void setUncaughtExceptionHandler(UncaughtExceptionHandler handler) {
		this.uncaughtExceptionHandler.set(handler);
	}

	public boolean isTrackThreads() {
		return trackThreads.get();
	}

	public void setTrackThreads(boolean trackThreads) {
		this.trackThreads.set(trackThreads);
	}

	public List<Thread> getAliveThreads() {
		return getThreads(true);
	}

	@Override public Thread newThread(Runnable r) {
		//
		final Thread t = new Thread(r);
		t.setDaemon(isDaemon());

		//
		String prefix = this.name;
		if (prefix == null || "".equals(prefix)) {
			prefix = getInvoker(2);
		}
		t.setName(prefix + "-" + getSequence(prefix));

		//
		final UncaughtExceptionHandler handler = this.getUncaughtExceptionHandler();
		if (handler != null) {
			t.setUncaughtExceptionHandler(handler);
		} else {
			t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
				@Override public void uncaughtException(Thread t, Throwable e) {
					LOG.error("unhandled exception in thread: " + t.getId() + ":" + t.getName(), e);
				}
			});
		}

		//
		if (this.isTrackThreads()) {
			addThread(t);
		}

		//
		return t;
	}

	protected String getInvoker(int depth) {
		final Exception e = new Exception();
		final StackTraceElement[] stes = e.getStackTrace();
		if (stes.length > depth) {
			return ClassUtils.getShortClassName(stes[depth].getClassName());
		} else {
			return getClass().getSimpleName();
		}
	}

	protected long getSequence(String invoker) {
		AtomicLong r = this.sequences.get(invoker);
		if (r == null) {
			r = new AtomicLong(0);
			AtomicLong existing = this.sequences.putIfAbsent(invoker, r);
			if (existing != null) {
				r = existing;
			}
		}
		return r.incrementAndGet();
	}

	protected synchronized void addThread(Thread thread) {
		//
		for (Iterator<WeakReference<Thread>> iter = this.threads.iterator(); iter.hasNext(); ) {
			Thread t = iter.next().get();
			if (t == null) {
				iter.remove();
			}
		}

		//
		this.threads.add(new WeakReference<Thread>(thread));
	}

	protected synchronized List<Thread> getThreads(boolean aliveOnly) {
		final List<Thread> r = new LinkedList<Thread>();
		for (Iterator<WeakReference<Thread>> iter = this.threads.iterator(); iter.hasNext(); ) {
			Thread t = iter.next().get();
			if (t == null) {
				iter.remove();
			} else if (!aliveOnly || t.isAlive()) {
				r.add(t);
			}
		}
		return r;
	}
}
