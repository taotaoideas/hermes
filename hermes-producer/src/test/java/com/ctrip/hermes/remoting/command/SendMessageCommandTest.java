package com.ctrip.hermes.remoting.command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.remoting.command.SendMessageCommand.MessageRawDataBatch;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageCommandTest {

	@Test
	public void testEncodeAndDecodeWithSingleMessage() {
		SendMessageCommand cmd = new SendMessageCommand();

		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("1", 1);

		SettableFuture<SendResult> future = SettableFuture.create();
		cmd.addMessage(createProducerMessage("topic", "body", "key", "partition", 100, true, properties), future);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decoded = new SendMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decoded.parse(buf, header);

		Map<Triple<String, Integer, Boolean>, MessageRawDataBatch> messageRawDataBatches = decoded
		      .getMessageRawDataBatches();

		Triple<String, Integer, Boolean> key = new Triple<>("topic", 100, true);
		Assert.assertEquals(1, messageRawDataBatches.size());
		Assert.assertTrue(messageRawDataBatches.containsKey(key));

		MessageRawDataBatch batch = messageRawDataBatches.get(key);

		Assert.assertEquals(1, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(0));

		List<ProducerMessage<?>> messages = batch.getMessages();
		Assert.assertEquals(1, messages.size());

		ProducerMessage<?> msg = messages.get(0);
		Assert.assertTrue(msg.getBornTime() != 0L);
		Assert.assertEquals(0, msg.getMsgSeqNo());
		Assert.assertEquals(100, msg.getPartitionNo());
		Assert.assertEquals("body", msg.getBody());
		Assert.assertEquals("key", msg.getKey());
		Assert.assertEquals("partition", msg.getPartition());
		Assert.assertEquals("topic", msg.getTopic());

		Map<String, Object> prop = msg.getProperties();
		Assert.assertEquals(1, prop.size());
		Assert.assertEquals(Integer.valueOf(1), prop.get("1"));

	}

	@Test
	public void testEncodeAndDecodeWithMultipleMessagesInSameTPP() {
		SendMessageCommand cmd = new SendMessageCommand();

		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("1", 1);

		SettableFuture<SendResult> future = SettableFuture.create();
		cmd.addMessage(createProducerMessage("topic", "body1", "key1", "partition1", 100, true, properties), future);
		cmd.addMessage(createProducerMessage("topic", "body2", "key2", "partition2", 100, true, properties), future);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decoded = new SendMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decoded.parse(buf, header);

		Map<Triple<String, Integer, Boolean>, MessageRawDataBatch> messageRawDataBatches = decoded
		      .getMessageRawDataBatches();

		Triple<String, Integer, Boolean> key = new Triple<>("topic", 100, true);
		Assert.assertEquals(1, messageRawDataBatches.size());
		Assert.assertTrue(messageRawDataBatches.containsKey(key));

		MessageRawDataBatch batch = messageRawDataBatches.get(key);

		Assert.assertEquals(2, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(0));
		Assert.assertTrue(batch.getMsgSeqs().contains(1));

		List<ProducerMessage<?>> messages = batch.getMessages();
		Assert.assertEquals(2, messages.size());

		// msg1
		ProducerMessage<?> msg = messages.get(0);
		Assert.assertTrue(msg.getBornTime() != 0L);
		Assert.assertEquals(0, msg.getMsgSeqNo());
		Assert.assertEquals(100, msg.getPartitionNo());
		Assert.assertEquals("body1", msg.getBody());
		Assert.assertEquals("key1", msg.getKey());
		Assert.assertEquals("partition1", msg.getPartition());
		Assert.assertEquals("topic", msg.getTopic());

		Map<String, Object> prop = msg.getProperties();
		Assert.assertEquals(1, prop.size());
		Assert.assertEquals(Integer.valueOf(1), prop.get("1"));

		// msg2
		msg = messages.get(1);
		Assert.assertTrue(msg.getBornTime() != 0L);
		Assert.assertEquals(1, msg.getMsgSeqNo());
		Assert.assertEquals(100, msg.getPartitionNo());
		Assert.assertEquals("body2", msg.getBody());
		Assert.assertEquals("key2", msg.getKey());
		Assert.assertEquals("partition2", msg.getPartition());
		Assert.assertEquals("topic", msg.getTopic());

		prop = msg.getProperties();
		Assert.assertEquals(1, prop.size());
		Assert.assertEquals(Integer.valueOf(1), prop.get("1"));

	}

	@Test
	public void testEncodeAndDecodeWithMultipleMessagesInDifferentTPP() {
		SendMessageCommand cmd = new SendMessageCommand();

		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("1", 1);

		SettableFuture<SendResult> future = SettableFuture.create();
		cmd.addMessage(createProducerMessage("topic1", "body1", "key1", "partition1", 100, true, properties), future);
		cmd.addMessage(createProducerMessage("topic1", "body2", "key2", "partition2", 100, true, properties), future);

		cmd.addMessage(createProducerMessage("topic2", "body1", "key1", "partition1", 200, true, properties), future);
		cmd.addMessage(createProducerMessage("topic2", "body2", "key2", "partition2", 200, true, properties), future);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decoded = new SendMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decoded.parse(buf, header);

		Map<Triple<String, Integer, Boolean>, MessageRawDataBatch> messageRawDataBatches = decoded
		      .getMessageRawDataBatches();
		Assert.assertEquals(2, messageRawDataBatches.size());

		// tpp1 start
		Triple<String, Integer, Boolean> key = new Triple<>("topic1", 100, true);
		Assert.assertTrue(messageRawDataBatches.containsKey(key));

		MessageRawDataBatch batch = messageRawDataBatches.get(key);

		Assert.assertEquals(2, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(0));
		Assert.assertTrue(batch.getMsgSeqs().contains(1));

		List<ProducerMessage<?>> messages = batch.getMessages();
		Assert.assertEquals(2, messages.size());

		// msg1
		ProducerMessage<?> msg = messages.get(0);
		Assert.assertTrue(msg.getBornTime() != 0L);
		Assert.assertEquals(0, msg.getMsgSeqNo());
		Assert.assertEquals(100, msg.getPartitionNo());
		Assert.assertEquals("body1", msg.getBody());
		Assert.assertEquals("key1", msg.getKey());
		Assert.assertEquals("partition1", msg.getPartition());
		Assert.assertEquals("topic1", msg.getTopic());

		Map<String, Object> prop = msg.getProperties();
		Assert.assertEquals(1, prop.size());
		Assert.assertEquals(Integer.valueOf(1), prop.get("1"));

		// msg2
		msg = messages.get(1);
		Assert.assertTrue(msg.getBornTime() != 0L);
		Assert.assertEquals(1, msg.getMsgSeqNo());
		Assert.assertEquals(100, msg.getPartitionNo());
		Assert.assertEquals("body2", msg.getBody());
		Assert.assertEquals("key2", msg.getKey());
		Assert.assertEquals("partition2", msg.getPartition());
		Assert.assertEquals("topic1", msg.getTopic());

		prop = msg.getProperties();
		Assert.assertEquals(1, prop.size());
		Assert.assertEquals(Integer.valueOf(1), prop.get("1"));
		// tpp1 end

		// ///////////////////////////////////////////////////////////////////////////

		// tpp2 start
		key = new Triple<>("topic2", 200, true);
		Assert.assertTrue(messageRawDataBatches.containsKey(key));

		batch = messageRawDataBatches.get(key);

		Assert.assertEquals(2, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(2));
		Assert.assertTrue(batch.getMsgSeqs().contains(3));

		messages = batch.getMessages();
		Assert.assertEquals(2, messages.size());

		// msg1
		msg = messages.get(0);
		Assert.assertTrue(msg.getBornTime() != 0L);
		Assert.assertEquals(2, msg.getMsgSeqNo());
		Assert.assertEquals(200, msg.getPartitionNo());
		Assert.assertEquals("body1", msg.getBody());
		Assert.assertEquals("key1", msg.getKey());
		Assert.assertEquals("partition1", msg.getPartition());
		Assert.assertEquals("topic2", msg.getTopic());

		prop = msg.getProperties();
		Assert.assertEquals(1, prop.size());
		Assert.assertEquals(Integer.valueOf(1), prop.get("1"));

		// msg2
		msg = messages.get(1);
		Assert.assertTrue(msg.getBornTime() != 0L);
		Assert.assertEquals(3, msg.getMsgSeqNo());
		Assert.assertEquals(200, msg.getPartitionNo());
		Assert.assertEquals("body2", msg.getBody());
		Assert.assertEquals("key2", msg.getKey());
		Assert.assertEquals("partition2", msg.getPartition());
		Assert.assertEquals("topic2", msg.getTopic());

		prop = msg.getProperties();
		Assert.assertEquals(1, prop.size());
		Assert.assertEquals(Integer.valueOf(1), prop.get("1"));
		// tpp2 end
	}

	public <T> ProducerMessage<T> createProducerMessage(String topic, T body, String key, String partition,
	      int partitionNo, boolean priority, Map<String, Object> properties) {
		ProducerMessage<T> msg = new ProducerMessage<T>(topic, body);
		msg.setBornTime(System.currentTimeMillis());
		msg.setKey(key);
		msg.setPartition(partition);
		msg.setPartitionNo(partitionNo);
		msg.setPriority(priority);
		msg.setProperties(properties);

		return msg;
	}
}
