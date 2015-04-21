package com.ctrip.hermes.adapter;

import com.ctrip.cmessaging.client.IMessage;
import com.ctrip.cmessaging.client.ISyncConsumer;
import com.ctrip.cmessaging.client.exception.ConsumeTimeoutException;
import com.ctrip.cmessaging.client.exception.IllegalExchangeName;
import com.ctrip.cmessaging.client.exception.IllegalTopic;
import com.ctrip.cmessaging.client.impl.ConsumerFactory;

public class TestUtil {

//	public static IMessage syncConsume(String id, String topic, String exchange) throws IllegalTopic,
//			  IllegalExchangeName,
//			  ConsumeTimeoutException {
//		IMessage message1 = null;
//		ISyncConsumer consumer1 =
//				  ConsumerFactory.instance.createConsumerAsSync
//							 (id, topic, exchange, 5000);
//		consumer1.setBatchSize(1);
//
//		message1 = consumer1.consumeOne();
//		// todo: dispose() here?
//		message1.dispose();
//
//		return message1;
//	}


	public static ISyncConsumer buildSyncConsumer(String topic, long timeout) throws IllegalTopic, IllegalExchangeName {
		ISyncConsumer consumer1 = ConsumerFactory.instance.createConsumerAsSync("922101_9dc4a4ff", topic,
				  "ExchangeTest", timeout);
		consumer1.setBatchSize(20);
		return consumer1;
	}
}
