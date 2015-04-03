package com.ctrip.hermes.adapter;

import com.ctrip.cmessaging.client.IAsyncConsumer;
import com.ctrip.cmessaging.client.IMessage;
import com.ctrip.cmessaging.client.content.AckMode;
import com.ctrip.cmessaging.client.event.IConsumerCallbackEventHandler;
import com.ctrip.cmessaging.client.exception.IllegalExchangeName;
import com.ctrip.cmessaging.client.exception.IllegalTopic;
import com.ctrip.cmessaging.client.impl.ConsumerFactory;

public class CmessageConsumerAsync {

	public static class MyEventHandler implements IConsumerCallbackEventHandler {

		@Override
		public void callback(IMessage message) throws Exception{
			System.out.println("MessageId:"+message.getMessageID());
			System.out.println("Body:"+new String(message.getBody()));
			System.out.println("header:"+message.getHeader());
			System.out.println();
			message.setAcks(AckMode.Ack);
			message.dispose();
		}
	}

	/**
	 * @param args
	 * @throws IllegalExchangeName
	 * @throws IllegalTopic
	 * @throws Exception
	 */
	public static void main(String[] args) throws IllegalTopic, IllegalExchangeName {

		/**
		 * change "com.ctrip.cmessaging.client.impl.ConsumerFactory" to "com.ctrip.hermes.adapter.impl.ConsumerFactory"
		 */
		IAsyncConsumer consumer1 = ConsumerFactory.instance.createConsumerAsAsync("900205_48db5650", "order_new",
				  "ExchangeTest");
		consumer1.setBatchSize(20);
		consumer1.addConsumerCallbackEventHandler(new MyEventHandler());
		consumer1.ConsumeAsync(1, false);

//		try {
//			Thread.sleep(20*1000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//
//		consumer1.stop();
	}

}
