package com.ctrip.hermes.broker.dal;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.xbean.propertyeditor.CollectionUtil;
import org.junit.Before;
import org.junit.Test;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessage;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageDao;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageEntity;
import com.ctrip.hermes.broker.dal.hermes.OffsetResend;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupId;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdDao;

public class StorageTest extends ComponentTestCase {

	private MessagePriorityDao msgDao;

	private OffsetMessageDao msgOffsetDao;

	private ResendGroupIdDao resendDao;

	private OffsetResend resendOffsetDao;

	private String topic = "order_new";

	private String groupName = "group1";

	private int groupId = 1;

	@Before
	public void before() {
		msgDao = lookup(MessagePriorityDao.class);
		msgOffsetDao = lookup(OffsetMessageDao.class);
		resendDao = lookup(ResendGroupIdDao.class);
		resendOffsetDao = lookup(OffsetResend.class);
	}

	@Test
	public void full() throws Exception {
		List<MessagePriority> wMsgs = makeMessages(5);
		for (MessagePriority wMsg : wMsgs) {
			appendMessage(wMsg);
		}
		List<MessagePriority> rMsgs = readMessage();

		assertEquals(wMsgs.size(), rMsgs.size());

		appendResend(rMsgs.get(1));

//		updateOffset(CollectionUtil.last(rMsgs));

		readResendMessage();
	}

	private void readResendMessage() {
	}

	private void updateOffset(MessagePriority msg) throws DalException {
		String topic = msg.getTopic();
		int partition = msg.getPartition();
		int priority = msg.getPriority();

		List<OffsetMessage> offset = msgOffsetDao.find(topic, partition, priority, groupId,
		      OffsetMessageEntity.READSET_FULL);

		if (offset.isEmpty()) {
			List<MessagePriority> topMsg = msgDao.top(topic, partition, priority, MessagePriorityEntity.READSET_FULL);
			long startOffset = 0;
			if (!topMsg.isEmpty()) {
				startOffset = topMsg.get(0).getId();
			}

			OffsetMessage initOffset = new OffsetMessage();
			initOffset.setCreationDate(new Date());
			initOffset.setGroupId(groupId);
			initOffset.setOffset(startOffset);
			initOffset.setPartition(partition);
			initOffset.setPriority(priority);
			initOffset.setTopic(topic);

			msgOffsetDao.insert(initOffset);
		}
	}

	private void appendResend(MessagePriority msg) throws DalException {
		ResendGroupId r = new ResendGroupId();

		r.setAttributes(msg.getAttributes());
		r.setCreationDate(new Date());
		r.setGroupId(groupId);
		r.setPartition(msg.getPartition());
		r.setPayload(msg.getPayload());
		r.setProducerId(msg.getProducerId());
		r.setProducerIp(msg.getProducerIp());
		r.setRefKey(msg.getRefKey());
		r.setRemainingRetries(5);
		r.setScheduleDate(new Date());
		r.setTopic(msg.getTopic());

		resendDao.insert(r);
	}

	private List<MessagePriority> readMessage() throws DalException {
		List<MessagePriority> msgs = msgDao.findIdAfter(topic, 0, 0, 0, 10, MessagePriorityEntity.READSET_FULL);

		for (MessagePriority msg : msgs) {
			msg.setTopic(topic);
			msg.setPartition(0);
			msg.setPriority(0);
		}

		return msgs;
	}

	private void appendMessage(MessagePriority msg) throws DalException {
		msgDao.insert(msg);
	}

	@Test
	public void testFind() throws Exception {
		List<MessagePriority> result = msgDao.findIdAfter(topic, 0, 0, 0, 10, MessagePriorityEntity.READSET_FULL);
		for (MessagePriority r : result) {
			System.out.println(r);
		}
	}

	@Test
	public void testInsert() throws Exception {
		MessagePriority m = makeMessage();

		msgDao.insert(m);
	}

	private List<MessagePriority> makeMessages(int count) {
		List<MessagePriority> result = new ArrayList<>();

		for (int i = 0; i < count; i++) {
			result.add(makeMessage());
		}

		return result;
	}

	private MessagePriority makeMessage() {
		MessagePriority m = new MessagePriority();
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
