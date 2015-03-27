package com.ctrip.hermes.broker;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.channel.MessageQueueManager;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.Tpp;
import com.ctrip.hermes.storage.MessageQueue;

public class DefaultMessageQueueManagerTest extends ComponentTestCase {

	@Test
	public void sameQueue() {
		MessageQueueManager m = lookup(MessageQueueManager.class);

		String topic = "order.new";

		MessageQueue q1 = m.findQueue(new Tpp(topic, 1, true));
		MessageQueue q2 = m.findQueue(new Tpp(topic, 1, true));
		assertTrue(q1.equals(q2));

	}

}
