package com.ctrip.hermes.spi.internal;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.ValveChain;
import com.ctrip.hermes.message.internal.MessageValve;
import com.dianping.cat.Cat;

public class TracingMessageValve implements MessageValve {
	public static final String ID = "tracing";

	@Override
	public void handle(ValveChain<Message<Object>> chain, PipelineContext<Message<Object>> ctx) {
		System.out.println("Tracing to cat");
		// Transaction t = Cat.newTransaction("Message", ctx.getMessage().getTopic());

		try {
			chain.handle(ctx);
		} catch (RuntimeException e) {
			Cat.logError(e);
			// t.setStatus(e);
		} finally {
			// t.complete();
		}
	}

}
