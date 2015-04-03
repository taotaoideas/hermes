package com.ctrip.hermes.adapter.old;

import com.ctrip.cmessaging.client.IProducer;
import com.ctrip.cmessaging.client.exception.IllegalExchangeName;
import com.ctrip.cmessaging.client.exception.IllegalSubject;
import com.ctrip.cmessaging.client.impl.ProducerFactory;

public class CmessageProducer {

	public static void main(String[] args) throws IllegalExchangeName, InterruptedException, IllegalSubject {
//		Config.setConfigWsUri("http://ws.config.framework.sh.ctripcorp.com/Configws/ServiceConfig/ConfigInfoes/Get/");
//		Config.setAppId("555555");

		/**
		 * 切换环境时，要注意所使用的exchangeName和identifier是否拥有那个环境的访问权限。
		 */
		IProducer producer = ProducerFactory.instance.create("ExchangeTest", "922101_9dc4a4ff");
		producer.PublishAsync("hello i3", "h.test.t");
		producer.PublishAsync("hello i23", "h.test.t");

		System.out.println("Send done.");
		System.exit(0);
	}
}
