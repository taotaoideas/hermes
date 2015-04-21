package com.ctrip.hermes.adapter;

import java.io.IOException;
import java.util.HashMap;

import com.ctrip.cmessaging.client.IProducer;
import com.ctrip.cmessaging.client.exception.IllegalExchangeName;
import com.ctrip.cmessaging.client.exception.IllegalSubject;
import com.ctrip.cmessaging.client.impl.MessageHeader;
import com.ctrip.cmessaging.client.impl.ProducerFactory;

public class CmessageProducer {

	public static void main(String[] args) throws IllegalExchangeName, InterruptedException, IllegalSubject, IOException {
//		Config.setConfigWsUri("http://ws.config.framework.sh.ctripcorp.com/Configws/ServiceConfig/ConfigInfoes/Get/");
//		Config.setAppId("555555");

		/**
		 * change "com.ctrip.cmessaging.client.impl.ProducerFactory" to "com.ctrip.hermes.adapter.impl.ProducerFactory"
		 */
		IProducer producer = ProducerFactory.instance.create("ExchangeTest", "922101_9dc4a4ff");
		MessageHeader header = new MessageHeader();
		header.setAppID("555999");
		header.setSequence("-4");
		header.setCorrelationID("7890123456");
		HashMap<String, String> map = new HashMap<>();
		map.put("myKey", "myValue");
		map.put("SOMETHING", "HERMES");
		header.setUserHeader(map);

		producer.PublishAsync("hello i3", "order_new", header);
//		producer.PublishAsync("hello i23", "order_new");

		System.out.println("Send done.");

		System.in.read();
	}
}
