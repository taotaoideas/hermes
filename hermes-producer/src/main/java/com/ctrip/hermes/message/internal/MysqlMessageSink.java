package com.ctrip.hermes.message.internal;

import java.util.concurrent.Future;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;

public class MysqlMessageSink implements PipelineSink<Future<SendResult>> {

	public MysqlMessageSink() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Future<SendResult> handle(PipelineContext<Future<SendResult>> ctx, Object input) {
		return null;
	}

}
