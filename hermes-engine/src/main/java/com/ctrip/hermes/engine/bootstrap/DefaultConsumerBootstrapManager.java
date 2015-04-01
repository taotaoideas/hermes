package com.ctrip.hermes.engine.bootstrap;

import java.util.Arrays;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerBootstrapManager.class)
public class DefaultConsumerBootstrapManager implements ConsumerBootstrapManager {

	@Inject
	private ConsumerBootstrapRegistry m_registry;

	public ConsumerBootstrap findConsumerBootStrap(String endpointType) {

		if (Arrays.asList(Endpoint.BROKER, Endpoint.TRANSACTION, Endpoint.KAFKA, Endpoint.LOCAL).contains(endpointType)) {
			return m_registry.findConsumerBootstrap(endpointType);
		} else {
			throw new IllegalArgumentException(String.format("unknow endpoint type: %s", endpointType));
		}

	}

}
