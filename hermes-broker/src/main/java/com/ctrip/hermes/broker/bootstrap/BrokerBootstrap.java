package com.ctrip.hermes.broker.bootstrap;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface BrokerBootstrap {
	public void start() throws Exception;

	public void stop() throws Exception;
}
