package com.ctrip.hermes.spi.internal;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.spi.Valve;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

public class TracingMessageValve implements Valve {
	public static final String ID = "tracing";

	@SuppressWarnings("unchecked")
	@Override
	public void handle(PipelineContext ctx, Object payload) {
		Message<Object> msg = (Message<Object>) payload;
		String topic = msg.getTopic();

		Transaction t = Cat.newTransaction("Message", topic);
		t.addData("key=" + msg.getKey());

		try {
			ctx.next(payload);
			t.setStatus("0");
		} catch (RuntimeException e) {
			Cat.logError(e);
			t.setStatus(e);
		} finally {
			t.complete();
		}
	}

}
