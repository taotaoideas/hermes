package com.ctrip.hermes.message.internal;

import java.util.concurrent.Future;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;

public class KafkaMessageSink implements PipelineSink<Future<SendResult>> {

	public KafkaMessageSink() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Future<SendResult> handle(PipelineContext<Future<SendResult>> ctx, Object msg) {
		// TODO Auto-generated method stub
		return null;
	}

}
