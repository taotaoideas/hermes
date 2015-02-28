package com.ctrip.hermes.channel;

import com.ctrip.hermes.storage.MessageQueue;

public interface MessageQueueManager {

	/**
	 * The Queue for Consumer
	 * @param topic
	 * @param groupId
	 * @return
	 */
	public MessageQueue findQueue(String topic, String groupId);

	/**
	 * The Queue for Producer
	 * @param topic
	 * @return
	 */
	public MessageQueue findQueue(String topic);

}
