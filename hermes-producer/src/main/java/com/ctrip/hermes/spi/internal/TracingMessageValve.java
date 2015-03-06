package com.ctrip.hermes.spi.internal;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.spi.Valve;
import com.dianping.cat.Cat;
import com.dianping.cat.CatConstants;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;

public class TracingMessageValve implements Valve {
	public static final String ID = "tracing";

	@SuppressWarnings("unchecked")
	@Override
	public void handle(PipelineContext ctx, Object payload) {
		Message<Object> msg = (Message<Object>) payload;
		String topic = msg.getTopic();

		Transaction t = Cat.newTransaction("Produce", topic);
		t.addData("key", msg.getKey());

		try {
			ctx.next(payload);

			MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
			String msgId = Cat.createMessageId();
			Cat.logEvent(CatConstants.TYPE_REMOTE_CALL, msgId);
			msg.addProperty(CatConstants.SERVER_MESSAGE_ID, msgId);
			msg.addProperty(CatConstants.CURRENT_MESSAGE_ID, tree.getMessageId());
			msg.addProperty(CatConstants.ROOT_MESSAGE_ID, tree.getRootMessageId());
			t.setStatus(Transaction.SUCCESS);
		} catch (RuntimeException e) {
			Cat.logError("Error send message", e);
			t.setStatus(e);
		} finally {
			t.complete();
		}
	}

}
