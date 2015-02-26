package com.ctrip.hermes.engine;

import java.util.List;

import com.ctrip.hermes.consumer.Message;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.spi.Valve;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

public class ConsumerTracingValve implements Valve {

	public static final String ID = "consumer-tracing";

	@SuppressWarnings("unchecked")
	@Override
	public void handle(PipelineContext ctx, Object payload) {
		List<Message<Object>> msg = (List<Message<Object>>) payload;
		String topic = ctx.get("topic");

		Transaction t = Cat.newTransaction("Consume", topic);

		StringBuilder keyData = new StringBuilder();
		// TODO should be multiple transaction?
		for (Message<Object> m : msg) {
			if (keyData.length() > 0) {
				keyData.append("&");
			}
			keyData.append("key=" + m.getKey());
		}
		t.addData(keyData.toString());

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
