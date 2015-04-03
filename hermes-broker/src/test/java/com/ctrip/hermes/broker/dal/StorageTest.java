package com.ctrip.hermes.broker.dal;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriority;
import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriorityEntity;
import com.ctrip.hermes.broker.dal.hermes.OTopicDao;
import com.ctrip.hermes.broker.dal.hermes.RTopicGroupid;
import com.ctrip.hermes.broker.dal.hermes.RTopicGroupidDao;
import com.ctrip.hermes.storage.util.CollectionUtil;

public class StorageTest extends ComponentTestCase {

	private MTopicPartitionPriorityDao msgDao;

	private OTopicDao msgOffsetDao;

	private RTopicGroupidDao resendDao;

	@Before
	public void before() {
		msgDao = lookup(MTopicPartitionPriorityDao.class);
		msgOffsetDao = lookup(OTopicDao.class);
		resendDao = lookup(RTopicGroupidDao.class);
	}

	@Test
	public void full() throws Exception {
		List<MTopicPartitionPriority> wMsgs = makeMessages(5);
		for (MTopicPartitionPriority wMsg : wMsgs) {
			appendMessage(wMsg);
		}
		List<MTopicPartitionPriority> rMsgs = readMessage();

		assertEquals(wMsgs.size(), rMsgs.size());

		appendResend(rMsgs.get(1));

		updateOffset(CollectionUtil.last(rMsgs));
	}

	private void updateOffset(MTopicPartitionPriority msg) {
   }

	private void appendResend(MTopicPartitionPriority msg) throws DalException {
		RTopicGroupid r = new RTopicGroupid();

		r.setAttributes(msg.getAttributes());
		r.setCreationDate(new Date());
		r.setPayload(msg.getPayload());
		r.setProducerId(msg.getProducerId());
		r.setProducerIp(msg.getProducerIp());
		r.setRefKey(msg.getRefKey());
		r.setRemainingRetries(5);
		r.setScheduleDate(new Date());

		resendDao.insert(r);
	}

	private List<MTopicPartitionPriority> readMessage() throws DalException {
		return msgDao.findIdAfter("order_new", 0, 0, 0, 10, MTopicPartitionPriorityEntity.READSET_FULL);
	}

	private void appendMessage(MTopicPartitionPriority msg) throws DalException {
		msgDao.insert(msg);
	}

	@Test
	public void testFind() throws Exception {
		List<MTopicPartitionPriority> result = msgDao.findIdAfter("order_new", 0, 0, 0, 10,
				MTopicPartitionPriorityEntity.READSET_FULL);
		for (MTopicPartitionPriority r : result) {
			System.out.println(r);
		}
	}

	@Test
	public void testInsert() throws Exception {
		MTopicPartitionPriority m = makeMessage();

		msgDao.insert(m);
	}

	private List<MTopicPartitionPriority> makeMessages(int count) {
		List<MTopicPartitionPriority> result = new ArrayList<>();

		for (int i = 0; i < count; i++) {
			result.add(makeMessage());
		}

		return result;
	}

	private MTopicPartitionPriority makeMessage() {
		MTopicPartitionPriority m = new MTopicPartitionPriority();
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
		m.setRefKey(uuid());
		m.setPartition(shard);
		m.setTopic(topic);
		return m;
	}

	private String uuid() {
		return UUID.randomUUID().toString();
	}

}
