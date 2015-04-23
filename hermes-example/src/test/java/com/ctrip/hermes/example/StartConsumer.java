package com.ctrip.hermes.example;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class StartConsumer extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		String topic = "order_new";

		Engine engine = lookup(Engine.class);

		Map<String, List<String>> subscribers = new HashMap<String, List<String>>();
		subscribers.put("group1", Arrays.asList("1-a"));

		for (Map.Entry<String, List<String>> entry : subscribers.entrySet()) {
			String groupId = entry.getKey();
			for (String id : entry.getValue()) {
				Subscriber s = new Subscriber(topic, groupId, new MyConsumer(id));
				System.out.println("Starting consumer " + groupId + ":" + id);
				engine.start(Arrays.asList(s));
			}

		}

		System.in.read();
	}

	static class MyConsumer implements Consumer<String> {

		private String m_id;

		public MyConsumer(String id) {
			m_id = id;
		}

		@Override
		public void consume(List<ConsumerMessage<String>> msgs) {
			for (ConsumerMessage<String> msg : msgs) {
				String body = msg.getBody();
				System.out.println(m_id + "<<< " + body);

				msg.ack();
			}
		}
	}

}
