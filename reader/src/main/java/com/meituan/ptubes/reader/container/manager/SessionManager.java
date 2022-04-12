package com.meituan.ptubes.reader.container.manager;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.network.connections.AbstractClientSession;
import com.meituan.ptubes.reader.container.network.connections.AsyncOpResListener;
import com.meituan.ptubes.reader.container.network.connections.IClientSessionFactory;
import com.meituan.ptubes.reader.container.network.request.sub.SubRequest;
import com.meituan.ptubes.reader.monitor.collector.AggregatedClientSessionStatMetricsCollector;
import com.meituan.ptubes.reader.monitor.collector.AggregatedReadTaskStatMetricsCollector;
import com.meituan.ptubes.reader.monitor.collector.ClientSessionStatMetricsCollector;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.producer.EventProducer;
import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import com.meituan.ptubes.reader.container.common.AbstractLifeCycle;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.storage.mem.buffer.BinlogInfoFactory;
import org.apache.commons.lang3.StringUtils;

/**
 * Session Manager, which contains a collection of tasks that respond to client requests and return data.
 */
public class SessionManager extends AbstractLifeCycle {

	private static final Logger LOG = LoggerFactory.getLogger(SessionManager.class);

	// writerTaskName map clientSession
	private final Map<String/*clientId*/, AbstractClientSession> clientSessionMap;
	private final IClientSessionFactory clientSessionFactory;
	private final AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector;
	private final AggregatedReadTaskStatMetricsCollector aggregatedReadTaskStatMetricsCollector;

	public SessionManager(IClientSessionFactory clientSessionFactory, AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector,
						  AggregatedReadTaskStatMetricsCollector aggregatedReadTaskStatMetricsCollector) {
		this.clientSessionMap = new ConcurrentHashMap<>(); /* weak consistency */
		this.clientSessionFactory = clientSessionFactory;
		this.aggregatedClientSessionStatMetricsCollector = aggregatedClientSessionStatMetricsCollector;
		this.aggregatedReadTaskStatMetricsCollector = aggregatedReadTaskStatMetricsCollector;
	}

	@Override
	public synchronized boolean start() {
		return super.start();
	}

	@Override
	public synchronized boolean stop() {
		boolean res = super.stop();

		if (res) {
			// concurrentHashMap weak consistency may cause a new session to enter when it stops
			Iterator<String> clientIdIterator = clientSessionMap.keySet().iterator();
			while (clientIdIterator.hasNext()) {
				String clientId = clientIdIterator.next();
				AbstractClientSession session = clientSessionMap.get(clientId);
				if (session != null) {
					synchronized (session) {
						session.closeAsync();
					}

					// View changes are guaranteed by the manager synchronization block
					clientIdIterator.remove();
					aggregatedClientSessionStatMetricsCollector.unregisterClientSessionStatMetricsCollector(clientId);
				}
			}

		}
		return res;
	}

	@Deprecated
	public synchronized AbstractClientSession getSession(String clientId) {
		return StringUtils.isBlank(clientId) ? null : clientSessionMap.get(clientId);
	}

	// Adding synchronized here will affect the response speed of client subscription
	public /*synchronized*/ void register(
		ReaderTaskManager readerTaskManager,
		String readTaskName,
		String writeTaskName,
		String clientId,
		Channel channel,
		SubRequest subRequest,
		AsyncOpResListener asyncOpResListener
	) {
		if (isStart()) {
			boolean res = true;
			try {
				AbstractClientSession session = clientSessionMap.get(clientId);
				if (session != null) {
					LOG.debug("session {} resub, channelId: {}", clientId, channel.id());
					synchronized (session) {
						session.resetAsynchronously(
							subRequest,
							asyncOpResListener
						);
					}
				} else {
					LOG.debug("bind session {} to channelId {}", clientId, channel.id());
					NettyUtil.setChannelAttribute(channel, ContainerConstants.ATTR_CLIENTID, clientId);
					ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector = aggregatedReadTaskStatMetricsCollector.getReaderTaskCollector(readTaskName);
					EventProducer producer = readerTaskManager.getEventProducer(readTaskName);
					StorageConfig storageConfig = producer.getStorageConfig();
					SourceType sourceType = producer.getSourceType();
					if (producer == null) {
						LOG.error("session {} want nonexistent reader task {}", clientId, readTaskName);
						res = false;
					} else {
						AbstractClientSession newSession = clientSessionFactory.produceSession(
							this,
							readTaskName,
							sourceType,
							clientId,
							channel,
							subRequest,
							storageConfig,
							readerTaskStatMetricsCollector
						);
						newSession.startup();

						// Attention: To prevent the producer from closing (especially when backtracking), the session subscription joins, either the producer closes first and the session join fails, or the session joins first, and the session is closed when the producer closes; and the sessionManager is not locked to prevent A session (re)subscription affects other sessions
						synchronized (producer) {
							if (producer.isRunning()) {
								clientSessionMap.put(
									clientId,
									newSession
								);
							} else {
								res = false;
							}
						}

						if (res) {
							LOG.debug("client {} channel {} settle in sessionManager:-D", clientId, channel.id());

							InetSocketAddress socketAddress = (InetSocketAddress) channel.remoteAddress();
							String ip = socketAddress.getAddress().getHostAddress();
							int port = socketAddress.getPort();
							ClientSessionStatMetricsCollector collector = aggregatedClientSessionStatMetricsCollector.registerClientSessionStatMetricsCollector(
								clientId,
								writeTaskName,
								ip,
								port,
								BinlogInfoFactory.newDefaultBinlogInfo(producer.getSourceType())
							);
							NettyUtil.setChannelAttribute(channel, ContainerConstants.ATTR_CLIENTSESSION_COLLECTOR, collector);
						} else {
							LOG.warn(
								"readerTask {} may be gone just now, session stop...",
								readTaskName
							);
							if (newSession != null) {
								synchronized (newSession) {
									newSession.closeAsync();
								}
							}
						}
					}

					if (asyncOpResListener != null) {
						asyncOpResListener.operationComplete(res);
					}
				}
			} catch (Throwable te) {
				LOG.error(
					"setup session for channel {} error",
					channel.id().asShortText(),
					te
				);
				if (asyncOpResListener != null) {
					asyncOpResListener.operationComplete(false);
				}
			}
		}
	}

	public void deregisterAsync(String clientId) {
		AbstractClientSession session = clientSessionMap.get(clientId);
		if (session != null) {
			synchronized (session) {
				// May conflict with the view in the sessionManager stop method
				session.closeAsync();
				clientSessionMap.remove(clientId);
				aggregatedClientSessionStatMetricsCollector.unregisterClientSessionStatMetricsCollector(clientId);
			}
		} else {
			LOG.info("session {} has gone, skip deregistering async", clientId);
		}
	}

	public void deregisterSessionsAsync(String readTaskName) {
		Iterator<String> clientIds = clientSessionMap.keySet().iterator();
		while (clientIds.hasNext()) {
			String clientId = clientIds.next();
			AbstractClientSession session = clientSessionMap.get(clientId);
			if (session != null && session.getReaderTaskName().equals(readTaskName)) {
				synchronized (session) {
					session.closeAsync();
					clientIds.remove();
					aggregatedClientSessionStatMetricsCollector.unregisterClientSessionStatMetricsCollector(clientId);
				}
			}
		}
	}

	public void deregisterSessionsInSync(String readerTaskName) {
		List<AbstractClientSession> sessions = new ArrayList(clientSessionMap.values());
		for (AbstractClientSession session : sessions) {
			if (session != null) {
				String sessionWithReaderTaskName = session.getReaderTaskName();
				if (StringUtils.equals(readerTaskName, sessionWithReaderTaskName)) {
					synchronized (session) {
						session.closeAsync();
					}
				}
			}
		}

		for (AbstractClientSession session : sessions) {
			if (session != null) {
				String clientId = session.getClientId();
				String sessionWithReaderTaskName = session.getReaderTaskName();
				if (StringUtils.equals(readerTaskName, sessionWithReaderTaskName)) {
					synchronized (session) {
						try {
							boolean res = session.waitForCloseWithTimeout(ContainerConstants.SESSION_SHUTDOWN_TIMEOUT_MILLSEC, TimeUnit.MILLISECONDS);
							if (res) {
								LOG.info("session {} close in sync successfully", clientId);
							} else {
								LOG.error("session {} close timeout: {} ms", clientId, ContainerConstants.SESSION_SHUTDOWN_TIMEOUT_MILLSEC);
							}
						} catch (InterruptedException ie) {
							LOG.error("interruption caught during deregistering session {}, force to release session resources", clientId, ie);
						} finally {
							clientSessionMap.remove(clientId);
							aggregatedClientSessionStatMetricsCollector.unregisterClientSessionStatMetricsCollector(clientId);
						}
					}
				}
			}
		}
	}
}
