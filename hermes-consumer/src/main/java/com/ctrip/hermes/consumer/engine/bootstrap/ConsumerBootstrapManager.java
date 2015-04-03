package com.ctrip.hermes.consumer.engine.bootstrap;



/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerBootstrapManager {

	public ConsumerBootstrap findConsumerBootStrap(String endpointType);

}
