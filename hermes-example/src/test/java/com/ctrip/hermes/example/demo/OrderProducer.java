package com.ctrip.hermes.example.demo;

import java.util.Random;
import java.util.UUID;

import com.ctrip.hermes.producer.Producer;

public class OrderProducer {

	private Random rnd = new Random(System.currentTimeMillis());

	public void send() {
		Order order = makeOrder();
		
		Producer.getInstance() //
		      .message("order.new", order) //
		      .withKey(order.getId()) //
		      .send();
	}

	private Order makeOrder() {
		String id = UUID.randomUUID().toString().substring(0, 6);
		return new Order(id, rnd.nextInt(10000));
	}
}
