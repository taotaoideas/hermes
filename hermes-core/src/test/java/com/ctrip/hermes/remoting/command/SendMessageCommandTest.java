package com.ctrip.hermes.remoting.command;

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.codec.JsonCodec;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.codec.DefaultMessageCodec;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.command.Header;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.Tpp;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageCommandTest extends ComponentTestCase {

	@Test
	public void testEncodePartialDecodecMessage() {
		Map<String, Object> appProperties = new HashMap<String, Object>();
		appProperties.put("1", 1);
		Map<String, Object> sysProperties = new HashMap<String, Object>();
		sysProperties.put("2", 2);
		ProducerMessage<String> msg = createProducerMessage("topic1", "body", "key", "partition", 100, true, null, null);

		DefaultMessageCodec codec = new DefaultMessageCodec("topic1");

		ByteBuf buf = Unpooled.buffer();
		codec.encode(msg, buf);
		ByteBuf exp = buf.duplicate();

		PartialDecodedMessage pdMsg = codec.partialDecode(buf);
		pdMsg.setSysProperties(null);

		ByteBuf buf2 = Unpooled.buffer();
		codec.encode(pdMsg, buf2);

		assertEquals(Arrays.toString(readByteBuf(exp)), Arrays.toString(readByteBuf(buf2)));
	}

	private byte[] readByteBuf(ByteBuf buf) {
		byte[] dst = new byte[buf.readableBytes()];
		buf.readBytes(dst);
		return dst;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEncodeAndDecodeWithSingleMessage() {
		JsonCodec jsonCodec = new JsonCodec();
		SendMessageCommand cmd = new SendMessageCommand();

		Map<String, Object> appProperties = new HashMap<String, Object>();
		appProperties.put("1", 1);
		Map<String, Object> sysProperties = new HashMap<String, Object>();
		sysProperties.put("2", 2);

		SettableFuture<SendResult> future = SettableFuture.create();
		cmd.addMessage(
		      createProducerMessage("topic", "body", "key", "partition", 100, true, appProperties, sysProperties), future);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decoded = new SendMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decoded.parse(buf, header);

		Map<Tpp, MessageRawDataBatch> messageRawDataBatches = decoded.getMessageRawDataBatches();

		Tpp tpp = new Tpp("topic", 100, true);
		Assert.assertEquals(1, messageRawDataBatches.size());
		Assert.assertTrue(messageRawDataBatches.containsKey(tpp));

		MessageRawDataBatch batch = messageRawDataBatches.get(tpp);

		Assert.assertEquals(1, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(0));

		List<PartialDecodedMessage> messages = batch.getMessages();
		Assert.assertEquals(1, messages.size());

		PartialDecodedMessage msg = messages.get(0);

		Assert.assertTrue(msg.getBornTime() != 0L);

		byte[] bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key", msg.getKey());

		Map<String, Object> decodedAppProperties = new HermesPrimitiveCodec(msg.getAppProperties()).readMap();
		Map<String, Object> decodedSysProperties = new HermesPrimitiveCodec(msg.getSysProperties()).readMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals(Integer.valueOf(1), decodedAppProperties.get("1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals(Integer.valueOf(2), decodedSysProperties.get("2"));

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEncodeAndDecodeWithMultipleMessagesInSameTPP() {
		JsonCodec jsonCodec = new JsonCodec();
		SendMessageCommand cmd = new SendMessageCommand();

		Map<String, Object> appProperties = new HashMap<String, Object>();
		appProperties.put("1", 1);
		Map<String, Object> sysProperties = new HashMap<String, Object>();
		sysProperties.put("2", 2);

		SettableFuture<SendResult> future = SettableFuture.create();
		cmd.addMessage(
		      createProducerMessage("topic", "body1", "key1", "partition", 100, true, appProperties, sysProperties),
		      future);
		cmd.addMessage(
		      createProducerMessage("topic", "body2", "key2", "partition", 100, true, appProperties, sysProperties),
		      future);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decoded = new SendMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decoded.parse(buf, header);

		Map<Tpp, MessageRawDataBatch> messageRawDataBatches = decoded.getMessageRawDataBatches();

		Tpp tpp = new Tpp("topic", 100, true);
		Assert.assertEquals(1, messageRawDataBatches.size());
		Assert.assertTrue(messageRawDataBatches.containsKey(tpp));

		MessageRawDataBatch batch = messageRawDataBatches.get(tpp);

		Assert.assertEquals(2, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(0));
		Assert.assertTrue(batch.getMsgSeqs().contains(1));

		List<PartialDecodedMessage> messages = batch.getMessages();
		Assert.assertEquals(2, messages.size());

		// msg1 start
		PartialDecodedMessage msg = messages.get(0);

		Assert.assertTrue(msg.getBornTime() != 0L);

		byte[] bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body1", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key1", msg.getKey());

		Map<String, Object> decodedAppProperties = new HermesPrimitiveCodec(msg.getAppProperties()).readMap();
		Map<String, Object> decodedSysProperties = new HermesPrimitiveCodec(msg.getSysProperties()).readMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals(Integer.valueOf(1), decodedAppProperties.get("1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals(Integer.valueOf(2), decodedSysProperties.get("2"));
		// msg1 end

		// msg2 start
		msg = messages.get(1);

		Assert.assertTrue(msg.getBornTime() != 0L);

		bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body2", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key2", msg.getKey());

		decodedAppProperties = new HermesPrimitiveCodec(msg.getAppProperties()).readMap();
		decodedSysProperties = new HermesPrimitiveCodec(msg.getSysProperties()).readMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals(Integer.valueOf(1), decodedAppProperties.get("1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals(Integer.valueOf(2), decodedSysProperties.get("2"));
		// msg1 end

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEncodeAndDecodeWithMultipleMessagesInDifferentTPP() {
		JsonCodec jsonCodec = new JsonCodec();
		SendMessageCommand cmd = new SendMessageCommand();

		Map<String, Object> appProperties = new HashMap<String, Object>();
		appProperties.put("1", 1);
		Map<String, Object> sysProperties = new HashMap<String, Object>();
		sysProperties.put("2", 2);

		SettableFuture<SendResult> future = SettableFuture.create();
		// tpp1
		cmd.addMessage(
		      createProducerMessage("topic1", "body1", "key1", "partition1", 100, true, appProperties, sysProperties),
		      future);
		cmd.addMessage(
		      createProducerMessage("topic1", "body2", "key2", "partition1", 100, true, appProperties, sysProperties),
		      future);
		// tpp2
		cmd.addMessage(
		      createProducerMessage("topic2", "body3", "key3", "partition2", 200, false, appProperties, sysProperties),
		      future);
		cmd.addMessage(
		      createProducerMessage("topic2", "body4", "key4", "partition2", 200, false, appProperties, sysProperties),
		      future);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decoded = new SendMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decoded.parse(buf, header);

		Map<Tpp, MessageRawDataBatch> messageRawDataBatches = decoded.getMessageRawDataBatches();
		Assert.assertEquals(2, messageRawDataBatches.size());

		// tpp1 start
		Tpp tpp = new Tpp("topic1", 100, true);
		Assert.assertTrue(messageRawDataBatches.containsKey(tpp));

		MessageRawDataBatch batch = messageRawDataBatches.get(tpp);

		Assert.assertEquals(2, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(0));
		Assert.assertTrue(batch.getMsgSeqs().contains(1));

		List<PartialDecodedMessage> messages = batch.getMessages();
		Assert.assertEquals(2, messages.size());

		// msg1 start
		PartialDecodedMessage msg = messages.get(0);

		Assert.assertTrue(msg.getBornTime() != 0L);

		byte[] bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body1", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key1", msg.getKey());

		Map<String, Object> decodedAppProperties = new HermesPrimitiveCodec(msg.getAppProperties()).readMap();
		Map<String, Object> decodedSysProperties = new HermesPrimitiveCodec(msg.getSysProperties()).readMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals(Integer.valueOf(1), decodedAppProperties.get("1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals(Integer.valueOf(2), decodedSysProperties.get("2"));
		// msg1 end

		// msg2 start
		msg = messages.get(1);

		Assert.assertTrue(msg.getBornTime() != 0L);

		bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body2", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key2", msg.getKey());

		decodedAppProperties = new HermesPrimitiveCodec(msg.getAppProperties()).readMap();
		decodedSysProperties = new HermesPrimitiveCodec(msg.getSysProperties()).readMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals(Integer.valueOf(1), decodedAppProperties.get("1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals(Integer.valueOf(2), decodedSysProperties.get("2"));
		// msg1 end
		// tpp1 end

		// tpp2 start
		tpp = new Tpp("topic2", 200, false);
		Assert.assertTrue(messageRawDataBatches.containsKey(tpp));

		batch = messageRawDataBatches.get(tpp);

		Assert.assertEquals(2, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(2));
		Assert.assertTrue(batch.getMsgSeqs().contains(3));

		messages = batch.getMessages();
		Assert.assertEquals(2, messages.size());

		// msg1 start
		msg = messages.get(0);

		Assert.assertTrue(msg.getBornTime() != 0L);

		bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body3", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key3", msg.getKey());

		decodedAppProperties = new HermesPrimitiveCodec(msg.getAppProperties()).readMap();
		decodedSysProperties = new HermesPrimitiveCodec(msg.getSysProperties()).readMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals(Integer.valueOf(1), decodedAppProperties.get("1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals(Integer.valueOf(2), decodedSysProperties.get("2"));
		// msg1 end

		// msg2 start
		msg = messages.get(1);

		Assert.assertTrue(msg.getBornTime() != 0L);

		bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body4", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key4", msg.getKey());

		decodedAppProperties = new HermesPrimitiveCodec(msg.getAppProperties()).readMap();
		decodedSysProperties = new HermesPrimitiveCodec(msg.getSysProperties()).readMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals(Integer.valueOf(1), decodedAppProperties.get("1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals(Integer.valueOf(2), decodedSysProperties.get("2"));
		// msg1 end
		// tpp2 end
	}

	public <T> ProducerMessage<T> createProducerMessage(String topic, T body, String key, String partition,
	      int partitionNo, boolean priority, Map<String, Object> appProperties, Map<String, Object> sysProperties) {
		ProducerMessage<T> msg = new ProducerMessage<T>(topic, body);
		msg.setBornTime(System.currentTimeMillis());
		msg.setKey(key);
		msg.setPartition(partition);
		msg.setPartitionNo(partitionNo);
		msg.setPriority(priority);
		msg.setAppProperties(appProperties);
		msg.setSysProperties(sysProperties);

		return msg;
	}
}
