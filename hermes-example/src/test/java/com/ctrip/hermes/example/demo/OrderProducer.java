package com.ctrip.hermes.example.demo;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;

import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;

public class OrderProducer {

	private Random rnd = new Random(System.currentTimeMillis());

	public Future<SendResult> send(String topic) {
		Order order = makeOrder(topic);
		System.out.println(String.format("%s >>>>>>>>>> %s", order, topic));

		return Producer.getInstance() //
		      .message(topic, order) //
		      .withKey(order.getId()) //
		      .send();
	}

	private Order makeOrder(String topic) {
		String id = UUID.randomUUID().toString().substring(0, 6);
		return new Order(topic + "." + id, rnd.nextInt(100));
	}
}
