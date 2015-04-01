package com.ctrip.hermes.engine.notifier;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.unidal.helper.Threads;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.engine.ConsumerContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerNotifier.class)
public class DefaultConsumerNotifier implements ConsumerNotifier {

	private Map<Long, Pair<ConsumerContext, ExecutorService>> m_consumerContexs = new ConcurrentHashMap<>();

	@Inject("consumer")
	protected Pipeline m_pipeline;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.engine.ConsumerPool#register(long, com.ctrip.hermes.engine.ConsumerContext)
	 */
	@Override
	public void register(long correlationId, ConsumerContext consumerContext) {
		// TODO configable thread pool
		m_consumerContexs.put(
		      correlationId,
		      new Pair<>(consumerContext, Threads.forPool().getFixedThreadPool(
		            "ConsumerThread-" + consumerContext.getTopic(), 10)));
	}

	@Override
	public void messageReceived(long correlationId, final List<ConsumerMessage<?>> msgs) {
		Pair<ConsumerContext, ExecutorService> pair = m_consumerContexs.get(correlationId);
		final ConsumerContext context = pair.getKey();
		ExecutorService executorService = pair.getValue();

		executorService.submit(new Runnable() {

			@Override
			public void run() {
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
