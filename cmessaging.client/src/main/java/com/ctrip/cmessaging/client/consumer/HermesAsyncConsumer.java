package com.ctrip.cmessaging.client.consumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.ctrip.cmessaging.client.IAsyncConsumer;
import com.ctrip.cmessaging.client.event.IConsumerCallbackEventHandler;
import com.ctrip.cmessaging.client.message.HermesIMessage;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class HermesAsyncConsumer implements IAsyncConsumer {

	private IConsumerCallbackEventHandler handler;

	private String topic;
	private String groupId;
	private boolean isAutoAck;

	public HermesAsyncConsumer(String topic, String groupId) {
		this.topic = topic;
		this.groupId = groupId;
	}

	@Override
	public void addConsumerCallbackEventHandler(IConsumerCallbackEventHandler iConsumerCallbackEventHandler) {
		handler = iConsumerCallbackEventHandler;
	}

	@Override
	public void ConsumeAsync() {
		this.ConsumeAsync(1, true);
	}

	@Override
	public void ConsumeAsync(int maxThread) {
		this.ConsumeAsync(maxThread, true);
	}

	@Override
	public void ConsumeAsync(Boolean autoAck) {
		this.ConsumeAsync(1, autoAck);
	}

	@Override
	public void ConsumeAsync(int maxThread, Boolean autoAck) {
		this.isAutoAck = autoAck;
		Engine engine = PlexusComponentLocator.lookup(Engine.class);

		List<Subscriber> subscribers = new ArrayList<>();
		maxThread = maxThread <= 0 ? 1: maxThread;

		for (int i = 0; i < maxThread; i ++) {
			subscribers.add(new Subscriber(topic, groupId, new InnerConsumer()));
		}

		engine.start(subscribers);
	}

	@Override
	public void stop() {
		// todo: do stop the consumers.
	}

	@Override
	public void topicBind(String topic, String exchangeName) {
		this.topic = topic;
		/*
		exchangeName is useless
		 */
	}

	@Override
	public void setBatchSize(int i) {
		/*
		do nothing
		 */
	}

	@Override
	public void setIdentifier(String identifier) {
		this.groupId = identifier;
	}

	class InnerConsumer implements Consumer<byte[]> {
		@Override
		public void consume(List<ConsumerMessage<byte[]>> msgs) {
			try {
//				System.out.println("received " + msgs.size() + " msgs.");
				for (ConsumerMessage<byte[]> msg : msgs) {
					handler.callback(new HermesIMessage(msg, isAutoAck));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
