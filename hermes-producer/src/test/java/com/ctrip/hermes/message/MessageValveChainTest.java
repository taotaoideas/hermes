package com.ctrip.hermes.message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.spi.MessageValve;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class MessageValveChainTest extends ComponentTestCase {

	@Test
	public void passAllValve() {
		MessageRegistry registry = lookup(MessageRegistry.class);

		final Message<Object> msg = new Message<Object>();
		msg.setBody(UUID.randomUUID().toString());

		final List<Integer> resultList = new ArrayList<Integer>();
		registry.registerValve(new MessageValve() {

			@Override
			public void handle(MessageValveChain chain, MessageContext ctx) {
				assertEquals(msg.getBody(), ctx.getMessage().getBody());
				resultList.add(10);
				chain.handle(ctx);
			}
		}, "", 10);
		registry.registerValve(new MessageValve() {

			@Override
			public void handle(MessageValveChain chain, MessageContext ctx) {
				assertEquals(msg.getBody(), ctx.getMessage().getBody());
				resultList.add(5);
				chain.handle(ctx);
			}
		}, "", 5);

		MessageValveChain chain = new MessageValveChain(registry.getValveList());
		MessageContext ctx = new MessageContext(msg);
		ctx.setSink(mock(MessageSink.class));
		chain.handle(ctx);

		assertEquals(Arrays.asList(5, 10), resultList);

	}

}
