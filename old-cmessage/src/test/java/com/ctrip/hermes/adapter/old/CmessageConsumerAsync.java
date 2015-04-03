package com.ctrip.hermes.adapter.old;

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
//		Config.setConfigWsUri("http://ws.config.framework.sh.ctripcorp.com/Configws/ServiceConfig/ConfigInfoes/Get/");
//		Config.setAppId("555555");

		IAsyncConsumer consumer1 = ConsumerFactory.instance.createConsumerAsAsync("900205_48db5650", "h.test.t", "ExchangeTest");
		consumer1.setBatchSize(20);
		consumer1.addConsumerCallbackEventHandler(new MyEventHandler());
		consumer1.ConsumeAsync(10, false);

//		try {
//			Thread.sleep(20*1000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//
//		consumer1.stop();
	}

}
