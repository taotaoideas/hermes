package com.ctrip.hermes.broker.dal.service;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.dal.MessageUtil;
import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessage;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.utils.CollectionUtil;

public class MessageServiceTest extends ComponentTestCase {

	private MessageService s;

	private String topic = "order_new";

	@Before
	public void before() {
		s = lookup(MessageService.class);
	}

	@Test
	public void test() throws Exception {
		int partition = 0;
		Tpp tpp = new Tpp(topic, partition, true);
		int groupId = 200;

		OffsetMessage lastOffset = s.findLastOffset(tpp, groupId);
		assertEquals(0L, lastOffset.getOffset());

		List<MessagePriority> rmsgs = s.read(tpp, lastOffset.getOffset() + 1, 10);
		assertEquals(0, rmsgs.size());

		List<MessagePriority> wmsgs = MessageUtil.makeMessages(tpp, 5);
		s.write(wmsgs);

		rmsgs = s.read(tpp, lastOffset.getOffset() + 1, 10);
		assertEquals(wmsgs.size(), rmsgs.size());

		s.updateOffset(lastOffset, CollectionUtil.last(rmsgs).getId());

		lastOffset = s.findLastOffset(tpp, groupId);
		assertEquals(CollectionUtil.last(rmsgs).getId(), lastOffset.getOffset());

		rmsgs = s.read(tpp, lastOffset.getOffset() + 1, 10);
		assertEquals(0L, rmsgs.size());

	}

}
