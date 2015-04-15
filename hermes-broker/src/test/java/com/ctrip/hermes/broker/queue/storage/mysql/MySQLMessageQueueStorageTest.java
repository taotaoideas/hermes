package com.ctrip.hermes.broker.queue.storage.mysql;

import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage.FetchResult;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.meta.entity.Storage;

public class MySQLMessageQueueStorageTest extends ComponentTestCase {

	private MySQLMessageQueueStorage s;

	@Before
	public void before() {
		s = (MySQLMessageQueueStorage) lookup(MessageQueueStorage.class, Storage.MYSQL);
	}

	@Test
	public void testFindLastOffset() throws Exception {
		Tpp tpp = new Tpp("order_new", 0, true);
		s.findLastOffset(tpp, 100);
	}

	@Test
	public void testFindLastResendOffset() throws Exception {
		Tpg tpg = new Tpg("order_new", 0, "group1");
		s.findLastResendOffset(tpg);
	}

	@Test
	public void testFetchMessages() throws Exception {
		Tpp tpp = new Tpp("order_new", 0, true);
		FetchResult result = s.fetchMessages(tpp, 0L, 10);
		ByteBuf out = Unpooled.buffer();
		result.getBatch().getTransferCallback().transfer(out);
		assertTrue(out.readableBytes() > 0);
		assertTrue(!result.getBatch().getMsgSeqs().isEmpty());
	}

	@Test
	public void testAppendMessages() throws Exception {
		String topic = "order_new";
		Tpp tpp = new Tpp(topic, 0, true);
		Collection<MessageRawDataBatch> batches = new ArrayList<>();
		// TODO mock data
		ByteBuf rawData = null;
		MessageRawDataBatch batch = new MessageRawDataBatch(topic, Arrays.asList(1), rawData);
		batches.add(batch);
		s.appendMessages(tpp, batches);
	}

	@Test
	public void testAckMessage() throws Exception {
		Tpp tpp = new Tpp("order_new", 0, true);
		s.ack(tpp, "group1", false, 222);
	}

	@Test
	public void testAckResend() throws Exception {
		Tpp tpp = new Tpp("order_new", 0, true);
		s.ack(tpp, "group1", true, 1);
	}

	@Test
	public void testNackMessage() throws Exception {
		Tpp tpp = new Tpp("order_new", 0, true);
		s.nack(tpp, "group1", false, Arrays.asList(1L, 2L));
	}

	@Test
	public void testNackResend() throws Exception {
		Tpp tpp = new Tpp("order_new", 0, true);
		s.nack(tpp, "group1", true, Arrays.asList(1L, 2L));
	}

}
