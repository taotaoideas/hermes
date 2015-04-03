package com.ctrip.hermes.adapter.old;


import com.ctrip.cmessaging.client.IMessage;
import com.ctrip.cmessaging.client.ISyncConsumer;
import com.ctrip.cmessaging.client.content.AckMode;
import com.ctrip.cmessaging.client.exception.ConsumeTimeoutException;
import com.ctrip.cmessaging.client.exception.IllegalExchangeName;
import com.ctrip.cmessaging.client.exception.IllegalTopic;
import com.ctrip.cmessaging.client.impl.ConsumerFactory;

public class CmessageConsumerSync {

	public static void main(String[] args) throws IllegalTopic, IllegalExchangeName, InterruptedException {



		ISyncConsumer consumer1 = ConsumerFactory.instance.
				  createConsumerAsSync("922101_9dc4a4ff", "h.test.t", "ExchangeTest", 3000);
		consumer1.setBatchSize(20);

		try {

			while(true) {
				IMessage message1 = consumer1.consumeOne();
				System.out.println("MessageId:" + message1.getMessageID());
				System.out.println("Body:" + new String(message1.getBody()));
				System.out.println();
				//业务逻辑
				//
				//根据业务需求设置Ack或Nack
				//message1.setAcks(AckMode.Ack);
//				message1.setAcks(AckMode.Nack);
				message1.dispose();

				Thread.sleep(2000);
			}
		} catch (ConsumeTimeoutException e) {
			e.printStackTrace();
		}
	}
}
