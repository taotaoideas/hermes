package com.ctrip.cmessaging.client.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import com.ctrip.cmessaging.client.IMessage;
import com.ctrip.cmessaging.client.ISyncConsumer;
import com.ctrip.cmessaging.client.exception.ConsumeTimeoutException;
import com.ctrip.cmessaging.client.message.HermesIMessage;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class HermesSyncConsumer implements ISyncConsumer {
	private String topic;
	private String groupId;


	BlockingDeque<ConsumerMessage<byte[]>> msgQueue = new LinkedBlockingDeque<>();

	public HermesSyncConsumer(String topic, String groupId) {
		this.topic = topic;
		this.groupId = groupId;

		Engine engine = PlexusComponentLocator.lookup(Engine.class);

		List<Subscriber> subscribers = new ArrayList<>();

		subscribers.add(new Subscriber(topic, groupId, new InnerConsumer()));

		// todo: move engine.start() into consumeOne() step,
		// need engine provides the interface to check whether it had been started.
		engine.start(subscribers);
	}


	@Override
	public IMessage consumeOne() throws ConsumeTimeoutException {
		try {
			ConsumerMessage<byte[]> consumerMessage = msgQueue.take();
			return new HermesIMessage(consumerMessage);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void setReceiveTimeout(long l) {
		// todo
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
			msgQueue.addAll(msgs);
		}
	}
}
