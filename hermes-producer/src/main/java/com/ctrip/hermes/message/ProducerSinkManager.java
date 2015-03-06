package com.ctrip.hermes.message;

import java.util.concurrent.Future;

import com.ctrip.hermes.channel.SendResult;

public interface ProducerSinkManager {
	public PipelineSink<Future<SendResult>> getSink(String topic);
}
