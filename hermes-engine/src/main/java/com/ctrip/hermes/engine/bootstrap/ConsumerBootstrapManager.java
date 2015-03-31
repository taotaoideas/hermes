package com.ctrip.hermes.engine.bootstrap;


/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerBootstrapManager {

	public ConsumerBootstrap findConsumerBootStrap(String endpointType);

}
