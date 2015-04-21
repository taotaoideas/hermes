package com.ctrip.hermes.adapter.UnitTest;


import org.junit.Test;

/*
对于 exchangeName在服务器端注册，之后其中的topic可随意写
对于未注册或不存在的exchangeName会有 java.lang.Exception: fail to get collector uri
对于错误的identifier, 却不会有异常
 */
public class IProducerFactoryTest {

	/**
	 * public IProducer create(String exchangeName, String identifier) throws IllegalExchangeName;
	 */
	@Test
	public void testThrowIllegalExchangeName() {
		// todo
	}

	@Test
	public void testWrongExchangeName() {
		// assert throws “java.lang.Exception: fail to get collector uri”
	}

}
