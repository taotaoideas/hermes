package com.ctrip.hermes.container;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.lookup.ContainerHolder;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.engine.ConsumerManager;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.NettyClient;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class DefaultConsumerManager extends ContainerHolder implements ConsumerManager {

	private Map<Integer, Consumer<?>> m_consumers = new ConcurrentHashMap<>();

	@Override
	public void startConsumer(Subscriber s) {
		NettyClient netty = lookup(NettyClient.class);

		Command cmd = new Command(CommandType.StartConsumerRequest) //
		      .addHeader("topic", s.getTopicPattern()) //
		      .addHeader("groupId", s.getGroupId());

		m_consumers.put(cmd.getCorrelationId(), s.getConsumer());

		netty.start(cmd);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void deliverMessage(int correlationId, List<Message<?>> msgs) {
		// TODO make it async
		Consumer consumer = m_consumers.get(correlationId);

		if (consumer != null) {
			List bodies = Lists.transform(msgs, new Function<Message, Object>() {

				@Override
				public Object apply(Message msg) {
					return msg.getBody();
				}

			});

			// TODO consumer pipeline
			consumer.consume(bodies);
		} else {
			// TODO
			System.out.println(String.format("Correlationid %s not found", correlationId));
		}
	}

}
