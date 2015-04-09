package com.ctrip.hermes.broker.queue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageQueueDumper implements MessageQueueDumper {
	private BlockingQueue<Triple<SettableFuture<Map<Integer, Boolean>>, MessageRawDataBatch, Boolean>> m_queue = new LinkedBlockingQueue<>();

	private Thread m_workerThread;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	protected String m_topic;

	protected int m_partition;

	public AbstractMessageQueueDumper(String topic, int partition) {
		m_topic = topic;
		m_partition = partition;

		m_workerThread = new Thread(new Runnable() {

			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					try {
						Triple<SettableFuture<Map<Integer, Boolean>>, MessageRawDataBatch, Boolean> data = m_queue.take();

						appendMessageSync(data.getFirst(), data.getMiddle(), data.getLast());

						// TODO
						// TimeUnit.MILLISECONDS.sleep(1);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						// TODO
						e.printStackTrace();
					}
				}
			}
		});
		// TODO
		m_workerThread.setDaemon(true);
		m_workerThread.setName(String.format("MessageQueueDumper-%s-%s-%d", this.getClass().getSimpleName(), topic,
		      partition));

	}

	public void submit(SettableFuture<Map<Integer, Boolean>> future, MessageRawDataBatch batch, boolean isPriority) {
		m_queue.add(new Triple<>(future, batch, isPriority));
	}

	public void startIfNecessary() {
		if (m_started.compareAndSet(false, true)) {
			m_workerThread.start();
		}
	}

	protected void appendMessageSync(SettableFuture<Map<Integer, Boolean>> future, MessageRawDataBatch batch,
	      boolean isPriority) {

		Map<Integer, Boolean> result = new HashMap<>();
		addResults(result, batch.getMsgSeqs(), false);

		doAppendMessageSync(batch, isPriority, result);

		future.set(result);
	}

	protected void addResults(Map<Integer, Boolean> result, List<Integer> seqs, boolean success) {
		for (Integer seq : seqs) {
			result.put(seq, success);
		}
	}

	protected void addResults(Map<Integer, Boolean> result, boolean success) {
		for (Integer key : result.keySet()) {
			result.put(key, success);
		}
	}

	protected abstract void doAppendMessageSync(MessageRawDataBatch batch, boolean isPriority,
	      Map<Integer, Boolean> result);

}
