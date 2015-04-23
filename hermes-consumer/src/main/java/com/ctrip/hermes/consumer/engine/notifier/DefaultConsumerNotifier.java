package com.ctrip.hermes.consumer.engine.notifier;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.pipeline.Pipeline;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerNotifier.class)
public class DefaultConsumerNotifier implements ConsumerNotifier {

	private ConcurrentMap<Long, Pair<ConsumerContext, ExecutorService>> m_consumerContexs = new ConcurrentHashMap<>();

	@Inject("consumer")
	protected Pipeline<Void> m_pipeline;

	@Override
	public void register(long correlationId, final ConsumerContext consumerContext) {
		// TODO configable thread pool
		m_consumerContexs.putIfAbsent(correlationId,
		      new Pair<>(consumerContext, Executors.newFixedThreadPool(10, new ThreadFactory() {

			      @Override
			      public Thread newThread(Runnable r) {
				      Thread t = new Thread(r);
				      t.setName("ConsumerThread-" + consumerContext.getTopic().getName());
				      return t;
			      }
		      })));
	}

	@Override
	public void deregister(long correlationId) {
		Pair<ConsumerContext, ExecutorService> pair = m_consumerContexs.remove(correlationId);
		pair.getValue().shutdown();
		return;
	}

	@Override
	public void messageReceived(final long correlationId, final List<ConsumerMessage<?>> msgs) {
		Pair<ConsumerContext, ExecutorService> pair = m_consumerContexs.get(correlationId);
		final ConsumerContext context = pair.getKey();
		ExecutorService executorService = pair.getValue();

		executorService.submit(new Runnable() {

			@SuppressWarnings("rawtypes")
			@Override
			public void run() {
				for (ConsumerMessage<?> msg : msgs) {
					if (msg instanceof BrokerConsumerMessage) {
						BrokerConsumerMessage bmsg = (BrokerConsumerMessage) msg;
						bmsg.setCorrelationId(correlationId);
						bmsg.setGroupId(context.getGroupId());
					}
				}

				m_pipeline.put(new Pair<ConsumerContext, List<ConsumerMessage<?>>>(context, msgs));
			}
		});

	}

	@Override
	public ConsumerContext find(long correlationId) {
		// TODO npe
		return m_consumerContexs.get(correlationId).getKey();
	}

}
