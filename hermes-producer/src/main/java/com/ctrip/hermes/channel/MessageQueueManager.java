package com.ctrip.hermes.channel;

import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.storage.MessageQueue;

public interface MessageQueueManager {

	public MessageQueue findQueue(String topic, String groupId, String partition);
	
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

	public Map<Pair<String, String>, MessageQueue> getQueues();

}
