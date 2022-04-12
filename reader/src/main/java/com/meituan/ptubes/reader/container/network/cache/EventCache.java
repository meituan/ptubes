package com.meituan.ptubes.reader.container.network.cache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

// Bad! uses a circular queue, see canal MemoryEventStoreWithBuffer -- marked
public class EventCache<T extends IEventCache> {

	private static final Logger LOG = LoggerFactory.getLogger(EventCache.class);

	// factor
	private int maxCacheEventNum;
	private int maxCacheSize;

	private long cacheEventNum;
	private long cacheSize;

	// linked list
	private volatile Node<T> head;
	private volatile Node<T> tail;

	private final ReentrantLock lock;
	private final Condition notFull;
	private final Condition notEmpty;

//	public static final Node SENTINEL_NODE = new Node(null);

	public EventCache(int maxCacheEventNum, int maxCacheSize) {
		this.maxCacheEventNum = maxCacheEventNum;
		this.maxCacheSize = maxCacheSize;
//		this.cacheSizeLowWaterMark = lowWaterMark;
//		this.cacheSizeHighWaterMark = highWaterMark;

		this.cacheEventNum = 0;
		this.cacheSize = 0;
		Node SENTINEL_NODE = new Node(null);
		this.head = SENTINEL_NODE;
		this.tail = SENTINEL_NODE;

		this.lock = new ReentrantLock();
		this.notFull = lock.newCondition();
		this.notEmpty = lock.newCondition();
	}

	private static class Node<T extends IEventCache> {
		private T item;
		private Node next = null;

		public Node(T item) {
			this.item = item;
		}

		public T getItem() {
			return item;
		}

		public void setItem(T item) {
			this.item = item;
		}

		public Node getNext() {
			return next;
		}

		public void setNext(Node next) {
			this.next = next;
		}
	}

	// The new node is placed at the end of the linked list, and the dequeued node is at the head of the linked list
	public void put(T data) throws InterruptedException {
		Node enqueueNode = new Node(data);
		this.lock.lockInterruptibly();
		try {
			// When multiple threads call put, if notFull.signal will wake up all waiting threads and re-scramble for the lock, so the condition needs to be re-judged
			while (/*cacheSize >= maxCacheSize || */cacheEventNum >= maxCacheEventNum) {
				this.notFull.await();
			}
			// join the list
			tail.setNext(enqueueNode);
			tail = enqueueNode;

			this.cacheSize += enqueueNode.getItem().getSize();
			this.cacheEventNum++;
			LOG.debug("current event cache size:{}", this.cacheEventNum);
			this.notEmpty.signal();
		} finally {
			this.lock.unlock();
		}
	}

	public boolean put(T data, long timeout, TimeUnit timeUnit) throws InterruptedException {
		Node enqueueNode = new Node(data);
		long timeNanos = timeUnit.toNanos(timeout);
		this.lock.lockInterruptibly();
		try {
			while (/*cacheSize >= maxCacheSize || */cacheEventNum >= maxCacheEventNum) {
				if (timeNanos < 0) {
					return false;
				}
				timeNanos = this.notFull.awaitNanos(timeNanos);
			}
			// join the list
			tail.setNext(enqueueNode);
			tail = enqueueNode;

			this.cacheSize += enqueueNode.getItem().getSize();
			this.cacheEventNum++;
			LOG.debug("current event cache size:{}", this.cacheEventNum);
			this.notEmpty.signal();
		} finally {
			this.lock.unlock();
		}
		return true;
	}

	public T get() throws InterruptedException {
		T ans = null;

		this.lock.lockInterruptibly();
		try {
			while (this.cacheEventNum == 0) {
				this.notEmpty.await();
			}
			// get data from head
			Node dequeueNode = this.head.next;
			this.head.next = null; // unlink
			this.head = dequeueNode;

			this.cacheSize -= dequeueNode.getItem().getSize();
			this.cacheEventNum--;
			ans = (T) dequeueNode.getItem();
			dequeueNode.setItem(null); // free space
			LOG.debug("current event cache size:{}", this.cacheEventNum);
			this.notFull.signal();
		} finally {
			this.lock.unlock();
		}

		return ans;
	}

	public T get(long timeout, TimeUnit timeUnit) throws InterruptedException {
		T ans = null;

		long timeNanos = timeUnit.toNanos(timeout);
		this.lock.lockInterruptibly();
		try {
			while (this.cacheEventNum == 0) {
				if (timeNanos < 0) {
					return ans;
				}
				timeNanos = this.notEmpty.awaitNanos(timeNanos);
			}

			Node dequeueNode = this.head.next;
			this.head.next = null; // unlink
			this.head = dequeueNode;

			this.cacheSize -= dequeueNode.getItem().getSize();
			this.cacheEventNum--;
			ans = (T) dequeueNode.getItem();
			dequeueNode.setItem(null);
			LOG.debug("current event cache size:{}", this.cacheEventNum);
			this.notFull.signal();
		} finally {
			this.lock.unlock();
		}

		return ans;
	}

	public void clear() {
		this.lock.lock();
		try {
			this.cacheEventNum = 0;
			this.cacheSize = 0;
			if (this.head != null) {
				this.head.next = null;
			}
			Node SENTINEL_NODE = new Node(null);
			this.head = SENTINEL_NODE;
			this.tail = SENTINEL_NODE;
		} finally {
			this.lock.unlock();
		}
	}

//	public T peekAndGet(int maxExpectedSize) throws InterruptedException {
//
//	}

}
