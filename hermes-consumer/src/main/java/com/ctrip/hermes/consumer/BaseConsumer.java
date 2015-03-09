package com.ctrip.hermes.consumer;

import java.util.List;

import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.remoting.CatConstants;
import com.ctrip.hermes.storage.util.CollectionUtil;
import com.dianping.cat.Cat;
import com.dianping.cat.configuration.NetworkInterfaceManager;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;

public abstract class BaseConsumer<T> implements Consumer<T> {

	@Override
	public void consume(List<StoredMessage<T>> msgs) {
		if (CollectionUtil.notEmpty(msgs)) {
			String topic = msgs.get(0).getTopic();

			for (StoredMessage<T> msg : msgs) {
				Transaction t = Cat.newTransaction("Message.Consumed", topic);
				MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
				String rootMsgId = msg.getProperty(CatConstants.ROOT_MESSAGE_ID);
				String parentMsgId = msg.getProperty(CatConstants.CURRENT_MESSAGE_ID);
				String msgId = msg.getProperty(CatConstants.SERVER_MESSAGE_ID);
				tree.setRootMessageId(rootMsgId);
				tree.setParentMessageId(parentMsgId);
				tree.setMessageId(msgId);
				System.out.println(String.format("Consumer: %s %s %s", msgId, parentMsgId, rootMsgId));

				try {
					t.addData("topic", topic);
					t.addData("key", msg.getKey());
					t.addData("groupId", getGroupId());
					// TODO
					t.addData("appId", "demo-app");

					consume(msg);

					String type = "Consumer:" + NetworkInterfaceManager.INSTANCE.getLocalHostAddress() + ":" + getGroupId();
					Cat.logEvent(type, msg.getTopic(), Event.SUCCESS, "key=" + msg.getKey());
					Cat.logMetricForCount(msg.getTopic());
					t.setStatus(msg.isSuccess() ? Transaction.SUCCESS : "FAILED-WILL-RETRY");
				} catch (RuntimeException | Error e) {
					Cat.logError(e);
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
