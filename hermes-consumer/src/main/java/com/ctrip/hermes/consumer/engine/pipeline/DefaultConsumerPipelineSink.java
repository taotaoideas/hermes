package com.ctrip.hermes.consumer.engine.pipeline;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = PipelineSink.class, value = "consumer")
public class DefaultConsumerPipelineSink implements PipelineSink<Void>, Initializable {

	private ExecutorService m_executor;

	@Override
	public void initialize() throws InitializationException {
		// TODO
		m_executor = new ThreadPoolExecutor(10, 20, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(100),
		      new ThreadFactory() {
			      AtomicInteger seq = new AtomicInteger(0);

			      @Override
			      public Thread newThread(Runnable r) {
				      Thread t = new Thread(r);
				      t.setName("ConsumerThread-" + seq.incrementAndGet());
				      return t;
			      }
		      }, new CallerRunsPolicy());
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public Void handle(PipelineContext<Void> ctx, Object payload) {
		Pair<ConsumerContext, List<ConsumerMessage<?>>> pair = (Pair<ConsumerContext, List<ConsumerMessage<?>>>) payload;

		invokeConsumer(pair.getValue(), pair.getKey().getConsumer());

		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void invokeConsumer(final List<ConsumerMessage<?>> msgs, final Consumer consumer) {
		m_executor.submit(new Runnable() {

			@Override
			public void run() {
				consumer.consume(msgs);
			}
		});
	}

}
