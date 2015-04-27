package com.ctrip.hermes.broker.ack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.broker.ack.DefaultAckManager.Operation.Type;
import com.ctrip.hermes.broker.ack.internal.AckHolder;
import com.ctrip.hermes.broker.ack.internal.BatchResult;
import com.ctrip.hermes.broker.ack.internal.ContinuousRange;
import com.ctrip.hermes.broker.ack.internal.DefaultAckHolder;
import com.ctrip.hermes.broker.ack.internal.EnumRange;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = AckManager.class)
public class DefaultAckManager implements AckManager, Initializable {

	// TODO while consumer disconnect, clear holder and offset
	private ConcurrentMap<Triple<Tpp, String, Boolean>, AckHolder<Integer>> m_holders = new ConcurrentHashMap<>();

	private BlockingQueue<Operation> m_opQueue = new LinkedBlockingQueue<>();

	private Thread m_workerThread;

	@Inject
	private MessageQueueManager m_queueManager;

	@Inject
	private MetaService m_metaService;

	@Override
	public void initialize() throws InitializationException {
		m_workerThread = new Thread(new Runnable() {

			@Override
			public void run() {
				List<Operation> ops = new ArrayList<Operation>();
				while (!Thread.currentThread().isInterrupted()) {
					try {
						handleOperations(ops);

						for (Map.Entry<Triple<Tpp, String, Boolean>, AckHolder<Integer>> entry : m_holders.entrySet()) {
							AckHolder<Integer> holder = entry.getValue();
							Tpp tpp = entry.getKey().getFirst();
							String groupId = entry.getKey().getMiddle();
							boolean resend = entry.getKey().getLast();

							BatchResult<Integer> result = holder.scan();

							if (result != null) {
								ContinuousRange doneRange = result.getDoneRange();
								EnumRange<Integer> failRange = result.getFailRange();
								if (failRange != null) {
									try {
										m_queueManager.nack(tpp, groupId, resend, failRange.getOffsets());
									} catch (Exception e) {
										// TODO
									}
								}

								if (doneRange != null) {
									try {
										m_queueManager.ack(tpp, groupId, resend, doneRange.getEnd());
									} catch (Exception e) {

									}
								}
							}
						}

					} catch (Exception e) {
						// TODO
						e.printStackTrace();
					} finally {
						// TODO
						try {
							TimeUnit.MILLISECONDS.sleep(50);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				}
			}

			@SuppressWarnings("unchecked")
			private void handleOperations(List<Operation> ops) {
				try {
					if (ops.isEmpty()) {
						// TODO config batchSize
						m_opQueue.drainTo(ops);
					}

					if (ops.isEmpty()) {
						return;
					}

					for (Operation op : ops) {
						switch (op.getType()) {
						case ACK:
							m_holders.get(op.getKey()).acked((Long) op.getData(), true);
							break;
						case NACK:
							m_holders.get(op.getKey()).acked((Long) op.getData(), false);
							break;
						case DELIVERED:
							m_holders.get(op.getKey()).delivered((List<Pair<Long, Integer>>) op.getData(), op.getCreateTime());
							break;

						default:
							break;
						}
					}

					ops.clear();
				} catch (Exception e) {
					// TODO
					e.printStackTrace();
				}
			}
		});
		// TODO
		m_workerThread.setDaemon(true);
		m_workerThread.setName("AckManagerWorker");
		m_workerThread.start();
	}

	@Override
	public void delivered(Tpp tpp, String groupId, boolean resend, List<Pair<Long, Integer>> msgSeqs) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		ensureMapEntryExist(key);
		// TODO
		m_opQueue.offer(new Operation(key, Type.DELIVERED, msgSeqs));
	}

	private void ensureMapEntryExist(Triple<Tpp, String, Boolean> key) {
		if (!m_holders.containsKey(key)) {
			m_holders.putIfAbsent(key,
			      new DefaultAckHolder<Integer>(m_metaService.getAckTimeoutSeconds(key.getFirst().getTopic()) * 1000));
		}
	}

	@Override
	public void acked(Tpp tpp, String groupId, boolean resend, Map<Long, Integer> msgSeqs) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		ensureMapEntryExist(key);
		for (Long msgSeq : msgSeqs.keySet()) {
			// TODO
			m_opQueue.offer(new Operation(key, Type.ACK, msgSeq));
		}
	}

	@Override
	public void nacked(Tpp tpp, String groupId, boolean resend, Map<Long, Integer> msgSeqs) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		ensureMapEntryExist(key);
		for (Long msgSeq : msgSeqs.keySet()) {
			// TODO
			m_opQueue.offer(new Operation(key, Type.NACK, msgSeq));
		}
	}

	static class Operation {
		public enum Type {
			ACK, NACK, DELIVERED;
		}

		private Triple<Tpp, String, Boolean> m_key;

		private Object m_data;

		private Type m_type;

		private long m_createTime;

		Operation(Triple<Tpp, String, Boolean> key, Type type, Object data) {
			m_key = key;
			m_data = data;
			m_type = type;
			m_createTime = System.currentTimeMillis();
		}

		public Triple<Tpp, String, Boolean> getKey() {
			return m_key;
		}

		public Object getData() {
			return m_data;
		}

		public Type getType() {
			return m_type;
		}

		public long getCreateTime() {
			return m_createTime;
		}

	}
}
