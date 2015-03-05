package com.ctrip.hermes.spi.internal;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.spi.Valve;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

public class TracingMessageValve implements Valve, LogEnabled {
	public static final String ID = "tracing";

	private Logger m_logger;

	@SuppressWarnings("unchecked")
	@Override
	public void handle(PipelineContext ctx, Object payload) {
		Message<Object> msg = (Message<Object>) payload;
		String topic = msg.getTopic();

		Transaction t = Cat.newTransaction(topic, "Produce");
		t.addData("key=" + msg.getKey());

		try {
			ctx.next(payload);
			t.setStatus(Transaction.SUCCESS);
		} catch (RuntimeException e) {
			m_logger.error("Error send message", e);
			Cat.logError(e);
			t.setStatus(e);
		} finally {
			t.complete();
		}
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

}
