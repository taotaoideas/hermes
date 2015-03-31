package com.ctrip.hermes.engine;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerBootstrapManager {

	public ConsumerBootstrap findConsumerBootStrap(String endpointType);

}
