package com.ctrip.hermes.example.demo;

import java.util.Random;
import java.util.UUID;

import com.ctrip.hermes.producer.Producer;

public class OrderProducer {

	private Random rnd = new Random(System.currentTimeMillis());

	public void send() {
		String id = UUID.randomUUID().toString().substring(0, 6);
		Order order = new Order(id, rnd.nextInt(10000));
		Producer.getInstance().message("order.new", order).withKey(id).withPartition(id).send();
	}

}
