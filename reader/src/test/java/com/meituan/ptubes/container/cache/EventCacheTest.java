package com.meituan.ptubes.container.cache;

import com.meituan.ptubes.reader.container.network.cache.BinaryEvent;
import com.meituan.ptubes.reader.container.network.cache.EventCache;
import com.meituan.ptubes.reader.container.network.encoder.EncoderType;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import org.junit.Test;

public class EventCacheTest {

	private static final Logger LOG = LoggerFactory.getLogger(EventCacheTest.class);

	public static class PutThread extends Thread {
		private EventCache<BinaryEvent> eventCache;
		private volatile boolean isRunning = true;

		private static final AtomicInteger DATA_GENERATOR = new AtomicInteger(1);
		private static final Random random = new Random();

		public PutThread(EventCache eventCache) {
			this.eventCache = eventCache;
		}

		public void shutdown() {
			isRunning = false;
		}

		@Override
		public void run() {
			BinaryEvent nextEvent = null;
			while (isRunning) {
				try {
					if (nextEvent == null) {
						nextEvent = new BinaryEvent(0, (short)-1, null, EncoderType.RAW, (String.valueOf(DATA_GENERATOR.getAndIncrement())).getBytes(), null);
						Thread.sleep(random.nextInt(1000));
					}

					if (eventCache.put(nextEvent, 100, TimeUnit.MILLISECONDS)) {
						nextEvent = null;
					} else {
						LOG.info("cache is full");
					}
				} catch (Exception e) {
					LOG.info("put thread is interrupted", e);
				}
			}
		}
	}

	public static class GetThread extends Thread {
		private EventCache<BinaryEvent> eventCache;
		private volatile boolean isRunning = true;

		private static final Random random = new Random();

		public GetThread(EventCache eventCache) {
			this.eventCache = eventCache;
		}

		public void shutdown() {
			isRunning = false;
		}

		@Override
		public void run() {
			while (isRunning) {
				try {
					BinaryEvent event = eventCache.get(100, TimeUnit.MILLISECONDS);
					Thread.sleep(random.nextInt(100));
					if (event != null) {
						LOG.info("get event : {}", new String((byte[]) event.getEncodedData()), null);
					} else {
						LOG.info("cache is empty");
					}
				} catch (Exception e) {
					LOG.info("get thread is interrupted", e);
				}
			}
		}
	}

	@Test
	public void eventCacheTest() throws Exception {
		final EventCache<BinaryEvent> cache = new EventCache<>(10, 128);
		PutThread putThread = new PutThread(cache);
		GetThread getThread = new GetThread(cache);

		putThread.start();
		getThread.start();

		Thread.sleep(10000);

		putThread.shutdown();
		getThread.shutdown();

		putThread.join();
		getThread.join();
	}

}
