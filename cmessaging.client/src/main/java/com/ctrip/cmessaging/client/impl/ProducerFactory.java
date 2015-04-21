package com.ctrip.cmessaging.client.impl;

import com.ctrip.cmessaging.client.IProducer;
import com.ctrip.cmessaging.client.IProducerFactory;
import com.ctrip.cmessaging.client.exception.IllegalExchangeName;
import com.ctrip.cmessaging.client.producer.HermesProducer;

public class ProducerFactory implements IProducerFactory {
	public static final ProducerFactory instance = new ProducerFactory();

	private ProducerFactory() {}

	Config appConfig = new Config();

	@Override
	public IProducer create(String exchangeName, String identifier) throws IllegalExchangeName {
		return new HermesProducer(exchangeName, appConfig.getAppId());
	}
}
