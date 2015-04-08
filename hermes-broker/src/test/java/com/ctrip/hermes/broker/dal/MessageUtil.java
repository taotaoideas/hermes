package com.ctrip.hermes.broker.dal;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.core.bo.Tpp;

public class MessageUtil {

	public static List<MessagePriority> makeMessages(Tpp tpp, int count) {
		List<MessagePriority> result = new ArrayList<>();

		for (int i = 0; i < count; i++) {
			result.add(makeMessage(tpp));
		}

		return result;
	}

	public static MessagePriority makeMessage(Tpp tpp) {
		MessagePriority m = new MessagePriority();
		Random rnd = new Random();

		String attributes = uuid();
		Date creationDate = new Date();
		byte[] payload = uuid().getBytes();
		int producerId = rnd.nextInt(1000);
		String producerIp = uuid().substring(0, 10);

		m.setAttributes(attributes);
		m.setCreationDate(creationDate);
		m.setPayload(payload);
		m.setPriority(tpp.getPriorityInt());
		m.setProducerId(producerId);
		m.setProducerIp(producerIp);
		m.setRefKey(uuid());
		m.setPartition(tpp.getPartition());
		m.setTopic(tpp.getTopic());
		return m;
	}

	public static String uuid() {
		return UUID.randomUUID().toString();
	}

}
