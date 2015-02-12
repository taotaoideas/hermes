package com.ctrip.hermes.broker;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.storage.impl.StorageMessageQueue;

public class DefaultMessageQueueManagerTest extends ComponentTestCase {

	@Test
	public void sameQueue() {
		DefaultMessageQueueManager m = (DefaultMessageQueueManager) lookup(MessageQueueManager.class);

		String topic = "order.new";
		String groupId = "group1";
		
		StorageMessageQueue q1 = m.findQueue(topic, groupId);
		StorageMessageQueue q2 = m.findQueue(topic, groupId);
		assertTrue(q1.equals(q2));
		
		StorageMessageQueue q3 = m.findQueue(topic);
		StorageMessageQueue q4 = m.findQueue(topic);
		assertTrue(q3.equals(q4));
	}

}
