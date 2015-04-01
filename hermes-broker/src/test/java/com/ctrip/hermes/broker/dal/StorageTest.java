package com.ctrip.hermes.broker.dal;

import java.util.Date;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriority;
import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriorityDao;

public class StorageTest extends ComponentTestCase {

	private MTopicShardPriorityDao msgDao;

	@Before
	public void before() {
		msgDao = lookup(MTopicShardPriorityDao.class);
	}

	@Test
	public void test() throws Exception {
		MTopicShardPriority m = new MTopicShardPriority();
		Random rnd = new Random();

		String attributes = uuid();
		Date creationDate = new Date();
		byte[] payload = uuid().getBytes();
		int priority = 0;
		int producerId = rnd.nextInt(1000);
		String producerIp = uuid().substring(0, 10);
		int shard = 0;
		String topic = "order_new";

		m.setAttributes(attributes);
		m.setCreationDate(creationDate);
		m.setPayload(payload);
		m.setPriority(priority);
		m.setProducerId(producerId);
		m.setProducerIp(producerIp);
		m.setShard(shard);
		m.setTopic(topic);

		msgDao.insert(m);
	}

	private String uuid() {
		return UUID.randomUUID().toString();
	}

}
