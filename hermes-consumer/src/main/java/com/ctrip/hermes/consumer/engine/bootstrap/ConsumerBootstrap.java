package com.ctrip.hermes.consumer.engine.bootstrap;

import com.ctrip.hermes.consumer.engine.ConsumerContext;

public interface ConsumerBootstrap {

	public void start(ConsumerContext consumerContext);

	public void stop(ConsumerContext consumerContext);

}
