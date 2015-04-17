package com.ctrip.hermes.remoting.command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.transport.TransferCallback;
import com.ctrip.hermes.core.transport.command.ConsumeMessageCommand;
import com.ctrip.hermes.core.transport.command.Header;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ConsumeMessageCommandTest extends ComponentTestCase {

	@Test
	public void testEncodeAndDecode() {
		Map<String, String> appProperties = new HashMap<>();
		appProperties.put("app_key", "123");

		Map<String, String> sysProperties = new HashMap<>();
		sysProperties.put("sys_key", "abc");

		List<ProducerMessage<String>> msgs1_1 = new ArrayList<>();
		msgs1_1.add(createProducerMessage("topic1", "t1_body1_1", "t1_key1_1", "t1_p1_1", 1, true, appProperties,
		      sysProperties));
		msgs1_1.add(createProducerMessage("topic1", "t1_body2_1", "t1_key2_1", "t1_p2_1", 1, true, appProperties,
		      sysProperties));
		msgs1_1.add(createProducerMessage("topic1", "t1_body3_1", "t1_key3_1", "t1_p1_1", 1, true, appProperties,
		      sysProperties));

		List<ProducerMessage<String>> msgs1_2 = new ArrayList<>();
		msgs1_2.add(createProducerMessage("topic1", "t1_body1_2", "t1_key1_2", "t1_p1_2", 2, false, appProperties,
		      sysProperties));
		msgs1_2.add(createProducerMessage("topic1", "t1_body2_2", "t1_key2_2", "t1_p2_2", 2, false, appProperties,
		      sysProperties));
		msgs1_2.add(createProducerMessage("topic1", "t1_body3_2", "t1_key3_2", "t1_p1_2", 2, false, appProperties,
		      sysProperties));

		List<ProducerMessage<String>> msgs2 = new ArrayList<>();
		msgs2.add(createProducerMessage("topic2", "t2_body1", "t2_key1", "t2_p1", 1, true, appProperties, sysProperties));
		msgs2.add(createProducerMessage("topic2", "t2_body2", "t2_key2", "t2_p2", 1, true, appProperties, sysProperties));
		msgs2.add(createProducerMessage("topic2", "t2_body3", "t2_key3", "t2_p3", 1, true, appProperties, sysProperties));

		ConsumeMessageCommand cmd = new ConsumeMessageCommand();

		Pair<Long, Integer> pair1 = new Pair<>(1L, 0);
		Pair<Long, Integer> pair2 = new Pair<>(2L, 0);
		Pair<Long, Integer> pair3 = new Pair<>(3L, 0);

		List<Pair<Long, Integer>> msgSeqs = Arrays.asList(pair1, pair2, pair3);
		cmd.addMessage(1, Arrays.asList(createBatch("topic1", msgs1_1, msgSeqs, 1, true, true)));
		cmd.addMessage(2, Arrays.asList(createBatch("topic2", msgs2, msgSeqs, 1, true, false)));
		cmd.addMessage(3, Arrays.asList(createBatch("topic1", msgs1_2, msgSeqs, 2, false, false)));

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		ConsumeMessageCommand decodedCmd = new ConsumeMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		Map<Long, List<ConsumerMessage<?>>> result = new HashMap<>();
		for (Map.Entry<Long, List<TppConsumerMessageBatch>> entry : decodedCmd.getMsgs().entrySet()) {
			long correlationId = entry.getKey();
			List<TppConsumerMessageBatch> batches = entry.getValue();

			List<ConsumerMessage<?>> msgs = decodeBatches(batches, String.class);
			result.put(correlationId, msgs);
		}

		Assert.assertEquals(3, result.size());
		Assert.assertTrue(result.containsKey(1L));
		Assert.assertTrue(result.containsKey(2L));
		Assert.assertTrue(result.containsKey(3L));

		// batch 1
		List<ConsumerMessage<?>> cmsgList1 = result.get(1L);

		Assert.assertEquals(3, cmsgList1.size());

		for (int i = 0; i < 3; i++) {
			ConsumerMessage<?> cmsg = cmsgList1.get(i);
			Assert.assertEquals(0, ((BrokerConsumerMessage<?>) cmsg).getRemainingRetries());
			Assert.assertEquals(true, ((BrokerConsumerMessage<?>) cmsg).isResend());
			Assert.assertEquals("topic1", cmsg.getTopic());
			Assert.assertEquals(1, ((BrokerConsumerMessage<?>) cmsg).getPartition());
			Assert.assertEquals(true, ((BrokerConsumerMessage<?>) cmsg).isPriority());
			Assert.assertNotNull(cmsg.getBornTime());
			Assert.assertTrue(String.format("t1_body%d_1", (i + 1)).equals((String) cmsg.getBody()));
			Assert.assertTrue(String.format("t1_key%d_1", (i + 1)).equals(cmsg.getKey()));
			Assert.assertEquals(Long.valueOf(i + 1).longValue(), ((BrokerConsumerMessage<?>) cmsg).getMsgSeq());

			Assert.assertEquals("123", cmsg.getProperty("app_key"));
		}

		// batch 2
		List<ConsumerMessage<?>> cmsgList2 = result.get(2L);
		Assert.assertEquals(3, cmsgList2.size());
		for (int i = 0; i < 3; i++) {
			ConsumerMessage<?> cmsg = cmsgList2.get(i);
			Assert.assertEquals(0, ((BrokerConsumerMessage<?>) cmsg).getRemainingRetries());
			Assert.assertEquals(false, ((BrokerConsumerMessage<?>) cmsg).isResend());
			Assert.assertEquals("topic2", cmsg.getTopic());
			Assert.assertEquals(1, ((BrokerConsumerMessage<?>) cmsg).getPartition());
			Assert.assertEquals(true, ((BrokerConsumerMessage<?>) cmsg).isPriority());
			Assert.assertNotNull(cmsg.getBornTime());
			Assert.assertTrue(String.format("t2_body%d", (i + 1)).equals((String) cmsg.getBody()));
			Assert.assertTrue(String.format("t2_key%d", (i + 1)).equals(cmsg.getKey()));
			Assert.assertEquals(Long.valueOf(i + 1).longValue(), ((BrokerConsumerMessage<?>) cmsg).getMsgSeq());

			Assert.assertEquals("123", cmsg.getProperty("app_key"));
		}

		// batch 3
		List<ConsumerMessage<?>> cmsgList3 = result.get(3L);
		Assert.assertEquals(3, cmsgList3.size());
		for (int i = 0; i < 3; i++) {
			ConsumerMessage<?> cmsg = cmsgList3.get(i);
			Assert.assertEquals(0, ((BrokerConsumerMessage<?>) cmsg).getRemainingRetries());
			Assert.assertEquals(false, ((BrokerConsumerMessage<?>) cmsg).isResend());
			Assert.assertEquals("topic1", cmsg.getTopic());
			Assert.assertEquals(2, ((BrokerConsumerMessage<?>) cmsg).getPartition());
			Assert.assertEquals(false, ((BrokerConsumerMessage<?>) cmsg).isPriority());
			Assert.assertNotNull(cmsg.getBornTime());
			Assert.assertTrue(String.format("t1_body%d_2", (i + 1)).equals((String) cmsg.getBody()));
			Assert.assertTrue(String.format("t1_key%d_2", (i + 1)).equals(cmsg.getKey()));
			Assert.assertEquals(Long.valueOf(i + 1).longValue(), ((BrokerConsumerMessage<?>) cmsg).getMsgSeq());

			Assert.assertEquals("123", cmsg.getProperty("app_key"));
		}
	}

	@SuppressWarnings("rawtypes")
	private List<ConsumerMessage<?>> decodeBatches(List<TppConsumerMessageBatch> batches, Class<?> bodyClazz) {
		List<ConsumerMessage<?>> msgs = new ArrayList<>();
		for (TppConsumerMessageBatch batch : batches) {
			List<Pair<Long, Integer>> msgSeqs = batch.getMsgSeqs();
			ByteBuf batchData = batch.getData();

			MessageCodec codec = lookup(MessageCodec.class);

			for (int j = 0; j < msgSeqs.size(); j++) {
				BaseConsumerMessage baseMsg = codec.decode(batchData, bodyClazz);
				BrokerConsumerMessage brokerMsg = new BrokerConsumerMessage(baseMsg);
				brokerMsg.setPartition(batch.getPartition());
				brokerMsg.setPriority(batch.isPriority());
				brokerMsg.setResend(batch.isResend());
				brokerMsg.setMsgSeq(msgSeqs.get(j).getKey());

				msgs.add(brokerMsg);
			}
		}

		return msgs;
	}

	public <T> ProducerMessage<T> createProducerMessage(String topic, T body, String key, String partition,
	      int partitionNo, boolean priority, Map<String, String> durableProperties,
	      Map<String, String> volatileProperties) {
		ProducerMessage<T> msg = new ProducerMessage<T>(topic, body);
		msg.setBornTime(System.currentTimeMillis());
		msg.setKey(key);
		msg.setPartition(partition);
		msg.setPartitionNo(partitionNo);
		msg.setPriority(priority);
		PropertiesHolder propertiesHolder = new PropertiesHolder();
		for (Map.Entry<String, String> entry : durableProperties.entrySet()) {
			propertiesHolder.addDurableAppProperty(entry.getKey(), entry.getValue());
		}
		for (Map.Entry<String, String> entry : volatileProperties.entrySet()) {
			propertiesHolder.addVolatileProperty(entry.getKey(), entry.getValue());
		}
		msg.setPropertiesHolder(propertiesHolder);

		return msg;
	}

	private TppConsumerMessageBatch createBatch(String topic, List<ProducerMessage<String>> msgs,
	      List<Pair<Long, Integer>> msgSeqs, int partition, boolean priority, boolean resend) {
		TppConsumerMessageBatch batch = new TppConsumerMessageBatch();

		batch.setTopic(topic);
		batch.setPartition(partition);
		batch.setPriority(priority);
		batch.setResend(resend);
		batch.addMsgSeqs(msgSeqs);

		final ByteBuf buf = Unpooled.buffer();

		MessageCodec codec = lookup(MessageCodec.class);

		for (ProducerMessage<String> msg : msgs) {
			codec.encode(msg, buf);
		}

		batch.setTransferCallback(new TransferCallback() {

			@Override
			public void transfer(ByteBuf out) {
				out.writeBytes(buf);
			}
		});

		return batch;
	}
}
