package com.ctrip.hermes.engine;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerBootstrapRegistry {
	public void registerBootstrap(String endpointType, ConsumerBootstrap consumerBootstrap);

	public ConsumerBootstrap findConsumerBootstrap(String endpointType);
}
