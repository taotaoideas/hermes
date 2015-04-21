package com.ctrip.hermes.adapter.UnitTest;

import org.junit.Test;

import com.ctrip.cmessaging.client.IMessage;
import com.ctrip.cmessaging.client.ISyncConsumer;
import com.ctrip.cmessaging.client.exception.ConsumeTimeoutException;
import com.ctrip.cmessaging.client.exception.IllegalExchangeName;
import com.ctrip.cmessaging.client.exception.IllegalTopic;
import com.ctrip.hermes.adapter.TestUtil;

public class ISyncConsumerTest {
	final String TOPIC = "order_new";


	/**
	 * 测试会抛超时异常
	 */
	@Test(expected = ConsumeTimeoutException.class)
	public void testThrowConsumeTimeoutException() throws IllegalTopic, IllegalExchangeName, InterruptedException, ConsumeTimeoutException {

		ISyncConsumer consumer = TestUtil.buildSyncConsumer(TOPIC, 1000);
		IMessage message1 = consumer.consumeOne();
		message1.dispose();
	}
}
