package com.ctrip.hermes.consumer;

import java.util.List;

import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.storage.util.CollectionUtil;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

public abstract class BaseConsumer<T> implements Consumer<T> {

	@Override
	public void consume(List<StoredMessage<T>> msgs) {
		if (CollectionUtil.notEmpty(msgs)) {
			String topic = msgs.get(0).getTopic();

			for (StoredMessage<T> msg : msgs) {
				Transaction t = Cat.newTransaction("Consume", topic);
				try {
					t.addData("topic", topic);
					t.addData("key", msg.getKey());
					t.addData("groupId", getGroupId());
					// TODO
					t.addData("appId", "demo-app");

					consume(msg);

					t.setStatus(msg.isSuccess() ? Transaction.SUCCESS : "NACK");
				} catch (RuntimeException e) {
					t.setStatus(e);
				} finally {
					t.complete();
				}
			}

		}
	}

	protected String getGroupId() {
		Subscribe s = this.getClass().getAnnotation(Subscribe.class);

		if (s != null) {
			return s.groupId();
		} else {
			return "Unknown";
		}
	}

	protected abstract void consume(StoredMessage<T> msg);

}
