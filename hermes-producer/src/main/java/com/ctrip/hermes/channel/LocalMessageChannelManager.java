package com.ctrip.hermes.channel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.storage.MessageQueue;
import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.range.OffsetRecord;
import com.ctrip.hermes.storage.util.CollectionUtil;

public class LocalMessageChannelManager implements MessageChannelManager, LogEnabled {

	@Inject
	private MessageQueueManager m_queueManager;

	private Map<Pair<String, String>, List<ConsumerChannelHandler>> m_handlers = new HashMap<>();

	private Logger m_logger;

	@Override
	public synchronized ConsumerChannel newConsumerChannel(final String topic, final String groupId) {

		return new ConsumerChannel() {

			private MessageQueue m_q = m_queueManager.findQueue(topic, groupId);

			@Override
			public void close() {
				// TODO remove handler
			}

			@Override
			public void start(ConsumerChannelHandler handler) {
				synchronized (LocalMessageChannelManager.this) {
					Pair<String, String> pair = new Pair<>(topic, groupId);
					List<ConsumerChannelHandler> curHandlers = m_handlers.get(pair);

					if (curHandlers == null) {
						curHandlers = startQueuePuller(m_q);
						m_handlers.put(pair, curHandlers);
					}
					curHandlers.add(handler);
				}
			}

			@Override
			public void ack(List<OffsetRecord> recs) {
				m_logger.info("ACK..." + recs);
				m_q.ack(recs);
			}
		};

	}

	private List<ConsumerChannelHandler> startQueuePuller(final MessageQueue q) {
		// TODO
		final List<ConsumerChannelHandler> handlers = Collections
		      .synchronizedList(new ArrayList<ConsumerChannelHandler>());

		new Thread() {

			private int m_idx = 0;

			@Override
			public void run() {
				while (true) {
					List<Message> msgs = q.read(100);

					if (CollectionUtil.notEmpty(msgs)) {
						for (Message msg : msgs) {
							dispatchMessage(msg);
						}
					} else {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
						}
					}
				}
			}

			private void dispatchMessage(Message msg) {
				while (true) {
					if (handlers.isEmpty()) {
						// TODO shutdown and cleanup
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
						}
					} else {
						int curIdx = m_idx++;
						ConsumerChannelHandler handler = handlers.get(curIdx % handlers.size());
						if (handler.isOpen()) {
							// TODO handle exception
							handler.handle(Arrays.asList(msg));
							break;
						} else {
							m_logger.info(String.format("Remove closed consumer handler %s", handler));
							handlers.remove(handler);
							continue;
						}
					}
				}
			}

		}.start();

		return handlers;
	}

	@Override
	public ProducerChannel newProducerChannel(String topic) {
		final MessageQueue q = m_queueManager.findQueue(topic);

		// TODO
		return new ProducerChannel() {

			@Override
			public void send(List<Message> msgs) {
				q.write(msgs);
			}

			@Override
			public void close() {
				// TODO Auto-generated method stub

			}
		};
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

}
