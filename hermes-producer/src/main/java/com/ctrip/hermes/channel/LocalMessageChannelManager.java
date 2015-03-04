package com.ctrip.hermes.channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.storage.MessageQueue;
import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.range.OffsetRecord;
import com.ctrip.hermes.storage.util.CollectionUtil;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;

public class LocalMessageChannelManager implements MessageChannelManager, LogEnabled {

	public static final String ID = "local";

	@Inject
	private MessageQueueManager m_queueManager;

	private Map<Triple<String, String, String>, List<ConsumerChannelHandler>> m_handlers = new HashMap<>();

	private Logger m_logger;

	@Override
	public synchronized ConsumerChannel newConsumerChannel(final String topic, final String groupId) {
		return newConsumerChannel(topic, groupId, "invalid");
	}

	@Override
	public synchronized ConsumerChannel newConsumerChannel(final String topic, final String groupId,
	      final String partition) {
		return new ConsumerChannel() {

			private MessageQueue m_q = m_queueManager.findQueue(topic, groupId, partition);

			@Override
			public void close() {
				// TODO remove handler
			}

			@Override
			public void start(ConsumerChannelHandler handler) {
				synchronized (LocalMessageChannelManager.this) {
					Triple<String, String, String> triple = new Triple<>(topic, groupId, partition);
					List<ConsumerChannelHandler> curHandlers = m_handlers.get(triple);

					if (curHandlers == null) {
						curHandlers = startQueuePuller(m_q, topic);
						m_handlers.put(triple, curHandlers);
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

	private List<ConsumerChannelHandler> startQueuePuller(final MessageQueue q, final String topic) {
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
						dispatchMessage(msgs);
					} else {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
						}
					}
				}
			}

			private void dispatchMessage(List<Message> msgs) {
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
							Transaction t = Cat.newTransaction("Deliver", topic);

							try {
								appendCatEvent(t, msgs, topic);

								handler.handle(msgs);

								t.setStatus(Transaction.SUCCESS);
							} catch (Exception e) {
								// TODO handle exception
								m_logger.error("", e);
								t.setStatus(e);
							} finally {
								t.complete();
							}
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
	public ProducerChannel newProducerChannel(final String topic) {
		final MessageQueue q = m_queueManager.findQueue(topic);

		// TODO
		return new ProducerChannel() {

			@Override
			public void send(List<com.ctrip.hermes.message.Message<byte[]>> pMsgs) {
				Transaction t = Cat.newTransaction("Receive", topic);

				try {

					List<Message> cMsgs = new ArrayList<Message>();
					for (com.ctrip.hermes.message.Message<byte[]> pMsg : pMsgs) {
						cMsgs.add(new Message(pMsg));
					}

					appendCatEvent(t, cMsgs, topic);

					q.write(cMsgs);

					t.setStatus(Transaction.SUCCESS);
				} catch (Throwable e) {
					t.setStatus(e);
				} finally {
					t.complete();
				}
			}

			@Override
			public void close() {
				// TODO Auto-generated method stub

			}
		};
	}

	private void appendCatEvent(Transaction t, List<Message> msgs, String topic) {
		for (Message msg : msgs) {
			Cat.logEvent("Message", topic, Event.SUCCESS, msg.getKey());
		}
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

}
