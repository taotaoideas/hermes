package com.ctrip.hermes.message.internal;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ProducerMessage;

public class DefaultMessageSink implements PipelineSink<Future<SendResult>> {
	@Inject
	private MessageSender messageSender;

	@Override
	public Future<SendResult> handle(PipelineContext<Future<SendResult>> ctx, Object input) {
		ProducerMessage<?> msg = (ProducerMessage<?>) input;

		return messageSender.send(msg);

	}
}
