package com.ctrip.hermes.consumer.engine.pipeline;

import java.util.List;

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
public class DefaultConsumerPipelineSink implements PipelineSink<Void> {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Void handle(PipelineContext<Void> ctx, Object payload) {
		Pair<ConsumerContext, List<ConsumerMessage<?>>> pair = (Pair<ConsumerContext, List<ConsumerMessage<?>>>) payload;

		Consumer consumer = pair.getKey().getConsumer();
		List<ConsumerMessage<?>> msgs = pair.getValue();
		consumer.consume(msgs);

		return null;
	}

}
