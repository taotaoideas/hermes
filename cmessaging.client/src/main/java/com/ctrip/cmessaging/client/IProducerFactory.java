package com.ctrip.cmessaging.client;

import com.ctrip.cmessaging.client.exception.IllegalExchangeName;

public interface IProducerFactory {

	public IProducer create(String exchangeName, String identifier) throws IllegalExchangeName;
	
}
