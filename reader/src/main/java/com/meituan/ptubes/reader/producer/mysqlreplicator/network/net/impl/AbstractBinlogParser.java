package com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl;

import com.meituan.ptubes.reader.container.common.vo.Gtid;
import com.meituan.ptubes.reader.container.common.vo.GtidSet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventListener;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserListener;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.filter.BinlogEventFilter;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.GtidEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.RotateEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.TableMapEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.NopEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XThreadFactory;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractBinlogParser implements BinlogParser {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractBinlogParser.class);

	protected Thread worker;
	protected ThreadFactory threadFactory;
	protected BinlogEventFilter eventFilter;

	protected BinlogEventListener eventListener;
	protected boolean clearTableMapEventsOnRotate = true;

	/**
	 * Used to trigger some operations (such as logs, etc., not much logic) when the parser itself is started, stopped, or abnormal.
	 */
	protected final List<BinlogParserListener> parserListeners;
	protected final AtomicBoolean verbose = new AtomicBoolean(false);
	protected final AtomicBoolean running = new AtomicBoolean(false);
	protected final BinlogEventParser defaultParser = new NopEventParser();

	/**
	 * Parser used to specifically handle event events
	 * Different eventType corresponds to different parser, there are currently more than 20 implementations
	 */
	protected final BinlogEventParser[] parsers = new BinlogEventParser[128];
	protected String binlogFileName;
	protected GtidSet prevGtidSet;
	protected Context context;

	protected abstract void doParse() throws Exception;

	protected abstract void doStart() throws Exception;

	protected abstract void doStop(long timeout, TimeUnit unit) throws Exception;

	protected abstract void doStop() throws Exception;

	public AbstractBinlogParser() {
		this.threadFactory = new XThreadFactory("binlog-parser", false);
		this.parserListeners = new CopyOnWriteArrayList<BinlogParserListener>();
	}

	@Override
	public boolean isRunning() {
		return this.running.get();
	}

	@Override
	public void start() throws Exception {
		if (!this.running.compareAndSet(false, true)) {
			return;
		}

		doStart();

		this.worker = this.threadFactory.newThread(new Task());
		this.worker.start();

		notifyOnStart();
	}

	@Override
	public void stop(long timeout, TimeUnit unit)  throws Exception{
		if (!this.running.compareAndSet(true, false)) {
			return;
		}

		try {
			final long now = System.nanoTime();
			doStop(timeout, unit);
			timeout -= unit.convert(System.nanoTime() - now, TimeUnit.NANOSECONDS);

			if (timeout > 0) {
				unit.timedJoin(this.worker, timeout);
				this.worker = null;
			}
		} finally {
			notifyOnStop();
		}
	}

	public void stop() throws Exception {
		if (!this.running.compareAndSet(true, false)) {
			return;
		}

		try {
			doStop();
			this.worker.join();
			this.worker = null;
		} finally {
			notifyOnStop();
		}
	}

	public boolean isVerbose() {
		return this.verbose.get();
	}

	public void setVerbose(boolean verbose) {
		this.verbose.set(verbose);
	}

	public ThreadFactory getThreadFactory() {
		return threadFactory;
	}

	public void setThreadFactory(ThreadFactory tf) {
		this.threadFactory = tf;
	}

	public BinlogEventFilter getEventFilter() {
		return eventFilter;
	}

	@Override
	public void setEventFilter(BinlogEventFilter filter) {
		this.eventFilter = filter;
	}

	public BinlogEventListener getEventListener() {
		return eventListener;
	}

	@Override
	public void setEventListener(BinlogEventListener listener) {
		this.eventListener = listener;
	}

	public boolean isClearTableMapEventsOnRotate() {
		return clearTableMapEventsOnRotate;
	}

	public void setClearTableMapEventsOnRotate(boolean clearTableMapEventsOnRotate) {
		this.clearTableMapEventsOnRotate = clearTableMapEventsOnRotate;
	}

	@Override
	public void setContext(Context context) {
		this.context = context;
	}

	@Override
	public Context getContext() {
		return context;
	}

	public void clearEventParsers() {
		for (int i = 0; i < this.parsers.length; i++) {
			this.parsers[i] = null;
		}
	}

	public BinlogEventParser getEventParser(int type) {
		BinlogEventParser parser = this.parsers[type];
		if (parser == null) {
			parser = this.defaultParser;
		}
		return parser;
	}

	public BinlogEventParser unregisterEventParser(int type) {
		return this.parsers[type] = null;
	}

	public void registerEventParser(BinlogEventParser parser) {
		this.parsers[parser.getEventType()] = parser;
	}

	// maintain backwards compat
	@Deprecated
	public void registgerEventParser(BinlogEventParser parser) {
		this.registerEventParser(parser);
	}

	@Deprecated
	public BinlogEventParser unregistgerEventParser(int type) {
		return unregisterEventParser(type);
	}

	public void setEventParsers(List<BinlogEventParser> parsers) {
		clearEventParsers();
		if (parsers != null) {
			for (BinlogEventParser parser : parsers) {
				registerEventParser(parser);
			}
		}
	}

	@Override
	public List<BinlogParserListener> getParserListeners() {
		return new ArrayList<BinlogParserListener>(this.parserListeners);
	}

	@Override
	public boolean addParserListener(BinlogParserListener listener) {
		return this.parserListeners.add(listener);
	}

	@Override
	public boolean removeParserListener(BinlogParserListener listener) {
		return this.parserListeners.remove(listener);
	}

	@Override
	public void setParserListeners(List<BinlogParserListener> listeners) {
		this.parserListeners.clear();
		if (listeners != null) {
			this.parserListeners.addAll(listeners);
		}
	}

	public String getBinlogFileName() {
		return binlogFileName;
	}

	public GtidSet getPrevGtidSet() {
		return prevGtidSet;
	}

	private void notifyOnStart() {
		for (BinlogParserListener listener : this.parserListeners) {
			listener.onStart(this);
		}
	}

	private void notifyOnStop() {
		for (BinlogParserListener listener : this.parserListeners) {
			listener.onStop(this);
		}
	}

	private void notifyOnException(Exception exception) {
		for (BinlogParserListener listener : this.parserListeners) {
			listener.onException(this, exception);
		}
	}

	protected class Task implements Runnable {
		@Override
		public void run() {
			try {
				doParse();
			} catch (EOFException e) {
				LOG.error("failed to parse binlog", e);
			} catch (Exception e) {
				notifyOnException(e);
				LOG.error("failed to parse binlog", e);
			} finally {
				try {
					stop(0, TimeUnit.MILLISECONDS);
				} catch (Exception e) {
					LOG.error("failed to stop binlog parser", e);
				}
			}
		}
	}

	public class Context implements BinlogParserContext, BinlogEventListener {
		private String binlogFileName;
		private GtidSet prevGtidSet;
		private Gtid currGtid = null;
		private final Map<Long, TableMapEvent> tableMapEvents = new HashMap<Long, TableMapEvent>();
		private boolean checksumEnabled;

		public Context(AbstractBinlogParser parser, Context context) {
			this.binlogFileName = parser.getBinlogFileName();
			this.prevGtidSet = parser.getPrevGtidSet();
			if (context != null) {
				this.tableMapEvents.putAll(context.tableMapEvents);
			}
		}

		@Override
		public String toString() {
			return binlogFileName + " | " + tableMapEvents;
		}

		@Override
		public final String getBinlogFileName() {
			return binlogFileName;
		}

		public final void setBinlogFileName(String name) {
			this.binlogFileName = name;
		}

		@Override
		public GtidSet getPrevGtidSet() {
			return prevGtidSet;
		}

		public void setPrevGtidSet(GtidSet prevGtidSet) {
			this.prevGtidSet = prevGtidSet;
		}

		public Gtid getCurrGtid() {
			return currGtid;
		}

		public void setCurrGtid(Gtid currGtid) {
			this.currGtid = currGtid;
		}

		@Override
		public final BinlogEventListener getEventListener() {
			return this;
		}

		@Override
		public final TableMapEvent getTableMapEvent(long tableId) {
			return this.tableMapEvents.get(tableId);
		}

		@Override
		public void onEvents(BinlogEventV4 event) {
			if (event == null) {
				return;
			}

			if (event instanceof TableMapEvent) {
				final TableMapEvent tme = (TableMapEvent) event;
				this.tableMapEvents.put(tme.getTableId(), tme);
			} else if (event instanceof RotateEvent) {
				final RotateEvent re = (RotateEvent) event;
				this.binlogFileName = re.getBinlogFileName().toString();
				if (isClearTableMapEventsOnRotate()) {
					this.tableMapEvents.clear();
				}
			} else if (event instanceof GtidEvent) {
				GtidEvent ge = (GtidEvent) event;
				if (this.currGtid != null) {
					this.prevGtidSet.add(currGtid);
				}
				ge.setPrevGtidSet(this.prevGtidSet.toString());
				this.currGtid = new Gtid(ge.getUuid(), ge.getTransactionId());
			}

			try {
				// Context is an inner class that holds a reference to the outer class AbstractBinlogParser.eventListener
				AbstractBinlogParser.this.eventListener.onEvents(event);
			} catch (Exception e) {
				LOG.error("failed to notify binlog event listener, event: " + event, e);
			}
		}

		@Override public boolean getChecksumEnabled() {
			return this.checksumEnabled;
		}

		@Override public void setChecksumEnabled(boolean flag) {
			this.checksumEnabled = flag;
		}
	}
}
