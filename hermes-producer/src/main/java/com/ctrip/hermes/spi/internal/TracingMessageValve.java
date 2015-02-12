package com.ctrip.hermes.spi.internal;

import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.spi.Valve;
import com.dianping.cat.Cat;

public class TracingMessageValve implements Valve {
	public static final String ID = "tracing";

	@Override
	public void handle(PipelineContext ctx, Object payload) {
		// Transaction t = Cat.newTransaction("Message", ctx.getMessage().getTopic());

		try {
			ctx.next(payload);
		} catch (RuntimeException e) {
			Cat.logError(e);
			// t.setStatus(e);
		} finally {
			// t.complete();
		}
	}

}
