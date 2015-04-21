package com.ctrip.cmessaging.client.impl;

import com.ctrip.cmessaging.client.IAsyncConsumer;
import com.ctrip.cmessaging.client.IConsumerFactory;
import com.ctrip.cmessaging.client.ISyncConsumer;
import com.ctrip.cmessaging.client.exception.IllegalExchangeName;
import com.ctrip.cmessaging.client.exception.IllegalTopic;
import com.ctrip.cmessaging.client.consumer.HermesAsyncConsumer;
import com.ctrip.cmessaging.client.consumer.HermesSyncConsumer;

public class ConsumerFactory implements IConsumerFactory {

	public static final ConsumerFactory instance = new ConsumerFactory();

	private ConsumerFactory(){
	}

	public IAsyncConsumer createConsumerAsAsync(String identifier, String topic, String exchangeName) throws IllegalTopic, IllegalExchangeName {
		/*
		exchangeName is useless
		 */
		return new HermesAsyncConsumer(topic, identifier);
	}


	public ISyncConsumer createConsumerAsSync(String identifier, String topic, String exchangeName, long receiveTimeout) throws IllegalTopic, IllegalExchangeName {

		return new HermesSyncConsumer(topic, identifier, receiveTimeout);
	}

}
