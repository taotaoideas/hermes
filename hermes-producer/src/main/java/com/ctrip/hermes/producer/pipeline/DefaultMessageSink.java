package com.ctrip.hermes.producer.pipeline;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.producer.api.SendResult;
import com.ctrip.hermes.producer.sender.MessageSender;

public class DefaultMessageSink implements PipelineSink<Future<SendResult>> {
	@Inject
	private MessageSender messageSender;

	@Override
	public Future<SendResult> handle(PipelineContext<Future<SendResult>> ctx, Object input) {
		return messageSender.send((ProducerMessage<?>) input);

	}
}
