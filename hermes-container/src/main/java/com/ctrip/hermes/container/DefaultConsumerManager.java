package com.ctrip.hermes.container;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.lookup.ContainerHolder;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.NettyClient;

public class DefaultConsumerManager extends ContainerHolder implements ConsumerManager {

	private Map<Integer, Consumer<?>> m_consumers = new ConcurrentHashMap<Integer, Consumer<?>>();

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
	public void deliverMessage(int correlationId, Object msg) {
		Consumer<?> consumer = m_consumers.get(correlationId);

		if (consumer != null) {
			consumer.consume(new PipelineContext(msg));
		} else {
			// TODO
			System.out.println(String.format("Correlationid %s not found", correlationId));
		}
	}

}
