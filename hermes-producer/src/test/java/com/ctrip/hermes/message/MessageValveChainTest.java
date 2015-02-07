package com.ctrip.hermes.message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.message.internal.MessageValve;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class MessageValveChainTest extends ComponentTestCase {

	@SuppressWarnings("unchecked")
	@Test
	public void passAllValve() {
		ValveRegistry<Message<Object>> registry = lookup(ValveRegistry.class, "message");

		final Message<Object> msg = new Message<Object>();
		msg.setBody(UUID.randomUUID().toString());

		final List<Integer> resultList = new ArrayList<Integer>();
		registry.registerValve(new MessageValve() {

			@Override
			public void handle(ValveChain<Message<Object>> chain, PipelineContext<Message<Object>> ctx) {
				assertEquals(msg.getBody(), ctx.getMessage().getBody());
				resultList.add(10);
				chain.handle(ctx);
			}
		}, "", 10);
		registry.registerValve(new MessageValve() {

			@Override
			public void handle(ValveChain<Message<Object>> chain, PipelineContext<Message<Object>> ctx) {
				assertEquals(msg.getBody(), ctx.getMessage().getBody());
				resultList.add(5);
				chain.handle(ctx);
			}
		}, "", 5);

		ValveChain<Message<Object>> chain = new ValveChain<>(registry.getValveList(), mock(PipelineSink.class));
		PipelineContext<Message<Object>> ctx = new PipelineContext<>(msg);
		chain.handle(ctx);

		assertEquals(Arrays.asList(5, 10), resultList);

	}

}
