package com.ctrip.hermes.broker;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.channel.LocalMessageQueueManager;
import com.ctrip.hermes.channel.MessageQueueManager;
import com.ctrip.hermes.storage.MessageQueue;

public class DefaultMessageQueueManagerTest extends ComponentTestCase {

	@Test
	public void sameQueue() {
		LocalMessageQueueManager m = (LocalMessageQueueManager) lookup(MessageQueueManager.class);

		String topic = "order.new";
		String groupId = "group1";
		
		MessageQueue q1 = m.findQueue(topic, groupId);
		MessageQueue q2 = m.findQueue(topic, groupId);
		assertTrue(q1.equals(q2));
		
		MessageQueue q3 = m.findQueue(topic);
		MessageQueue q4 = m.findQueue(topic);
		assertTrue(q3.equals(q4));
	}

}
