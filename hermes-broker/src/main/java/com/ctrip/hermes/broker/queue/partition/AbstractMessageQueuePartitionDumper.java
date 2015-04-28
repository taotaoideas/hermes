package com.ctrip.hermes.broker.queue.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageQueuePartitionDumper implements MessageQueuePartitionDumper {
	private static final int DEFAULT_BATCH_SIZE = 10;

	private BlockingQueue<FutureBatchPriorityWrapper> m_queue = new LinkedBlockingQueue<>();

	private Thread m_workerThread;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	private int batchSize = DEFAULT_BATCH_SIZE;

	protected String m_topic;

	protected int m_partition;

	public AbstractMessageQueuePartitionDumper(String topic, int partition) {
		m_topic = topic;
		m_partition = partition;

		m_workerThread = new Thread(new Runnable() {

			@Override
			public void run() {
				List<FutureBatchPriorityWrapper> todos = new ArrayList<>();

				while (!Thread.currentThread().isInterrupted()) {
					try {
						if (todos.isEmpty()) {
							m_queue.drainTo(todos, batchSize);
						}

						if (!todos.isEmpty()) {
							appendMessageSync(todos);
							todos.clear();
						} else {
							TimeUnit.MILLISECONDS.sleep(50);
						}

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
		m_workerThread.setName(String.format("MessageQueuePartitionDumper-%s-%s-%d", this.getClass().getSimpleName(),
		      topic, partition));

	}

	public void submit(SettableFuture<Map<Integer, Boolean>> future, MessageRawDataBatch batch, boolean isPriority) {
		m_queue.offer(new FutureBatchPriorityWrapper(future, batch, isPriority));
	}

	public void startIfNecessary() {
		if (m_started.compareAndSet(false, true)) {
			m_workerThread.start();
		}
	}

	protected void appendMessageSync(List<FutureBatchPriorityWrapper> todos) {

		List<FutureBatchResultWrapper> priorityTodos = new ArrayList<>();
		List<FutureBatchResultWrapper> nonPriorityTodos = new ArrayList<>();

		for (FutureBatchPriorityWrapper todo : todos) {
			Map<Integer, Boolean> result = new HashMap<>();
			addResults(result, todo.getBatch().getMsgSeqs(), false);

			if (todo.isPriority()) {
				priorityTodos.add(new FutureBatchResultWrapper(todo.getFuture(), todo.getBatch(), result));
			} else {
				nonPriorityTodos.add(new FutureBatchResultWrapper(todo.getFuture(), todo.getBatch(), result));
			}
		}

		Function<FutureBatchResultWrapper, Pair<MessageRawDataBatch, Map<Integer, Boolean>>> transformFucntion = new Function<FutureBatchResultWrapper, Pair<MessageRawDataBatch, Map<Integer, Boolean>>>() {

			@Override
			public Pair<MessageRawDataBatch, Map<Integer, Boolean>> apply(FutureBatchResultWrapper input) {
				return new Pair<MessageRawDataBatch, Map<Integer, Boolean>>(input.getBatch(), input.getResult());
			}
		};

		doAppendMessageSync(true, Collections2.transform(priorityTodos, transformFucntion));

		doAppendMessageSync(false, Collections2.transform(nonPriorityTodos, transformFucntion));

		for (List<FutureBatchResultWrapper> todo : Arrays.asList(priorityTodos, nonPriorityTodos)) {
			for (FutureBatchResultWrapper fbw : todo) {
				SettableFuture<Map<Integer, Boolean>> future = fbw.getFuture();
				Map<Integer, Boolean> result = fbw.getResult();
				future.set(result);
			}
		}

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

	protected abstract void doAppendMessageSync(boolean isPriority,
	      Collection<Pair<MessageRawDataBatch, Map<Integer, Boolean>>> todos);

	private static class FutureBatchResultWrapper {
		private SettableFuture<Map<Integer, Boolean>> m_future;

		private MessageRawDataBatch m_batch;

		private Map<Integer, Boolean> m_result;

		public FutureBatchResultWrapper(SettableFuture<Map<Integer, Boolean>> future, MessageRawDataBatch batch,
		      Map<Integer, Boolean> result) {
			m_future = future;
			m_batch = batch;
			m_result = result;
		}

		public SettableFuture<Map<Integer, Boolean>> getFuture() {
			return m_future;
		}

		public MessageRawDataBatch getBatch() {
			return m_batch;
		}

		public Map<Integer, Boolean> getResult() {
			return m_result;
		}

	}

	private static class FutureBatchPriorityWrapper {
		private SettableFuture<Map<Integer, Boolean>> m_future;

		private MessageRawDataBatch m_batch;

		private boolean m_isPriority;

		public FutureBatchPriorityWrapper(SettableFuture<Map<Integer, Boolean>> future, MessageRawDataBatch batch,
		      boolean isPriority) {
			m_future = future;
			m_batch = batch;
			m_isPriority = isPriority;
		}

		public SettableFuture<Map<Integer, Boolean>> getFuture() {
			return m_future;
		}

		public MessageRawDataBatch getBatch() {
			return m_batch;
		}

		public boolean isPriority() {
			return m_isPriority;
		}

	}
}
