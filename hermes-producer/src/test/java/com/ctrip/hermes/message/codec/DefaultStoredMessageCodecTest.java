package com.ctrip.hermes.message.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.storage.storage.Offset;

public class DefaultStoredMessageCodecTest extends ComponentTestCase {

	@Test
	public void test() {
		StoredMessageCodec codec = lookup(StoredMessageCodec.class);

		List<StoredMessage<byte[]>> writeMsgs = new ArrayList<StoredMessage<byte[]>>();
		StoredMessage<byte[]> msg1 = new StoredMessage<byte[]>();
		msg1.setBody(new byte[] { 1, 2, 3 });
		msg1.setAckOffset(new Offset(UUID.randomUUID().toString(), 1));
		msg1.setKey(UUID.randomUUID().toString());
		msg1.setOffset(new Offset(UUID.randomUUID().toString(), 11));
		msg1.setPartition(UUID.randomUUID().toString());
		msg1.setPriority(true);
		msg1.setSuccess(true);
		msg1.setTopic(UUID.randomUUID().toString());
		msg1.addProperty(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		msg1.addProperty(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		writeMsgs.add(msg1);

		StoredMessage<byte[]> msg2 = new StoredMessage<byte[]>();
		msg2.setBody(new byte[] { 'a', 'b', 'c' });
		msg2.setAckOffset(new Offset(UUID.randomUUID().toString(), 2));
		writeMsgs.add(msg2);

		ByteBuffer buf = codec.encode(writeMsgs);

		buf.flip();
		List<StoredMessage<byte[]>> readMsgs = codec.decode(buf);

		assertTrue(writeMsgs.size() > 0);
		assertEquals(writeMsgs.size(), readMsgs.size());
		for (int i = 0; i < writeMsgs.size(); i++) {
			StoredMessage<byte[]> wm = writeMsgs.get(i);
			StoredMessage<byte[]> rm = readMsgs.get(i);

			assertArrayEquals(wm.getBody(), rm.getBody());
			assertEquals(wm.getAckOffset(), rm.getAckOffset());
			assertEquals(wm.getKey(), rm.getKey());
			assertEquals(wm.getOffset(), rm.getOffset());
			assertEquals(wm.getPartition(), rm.getPartition());
			assertEquals(wm.isPriority(), rm.isPriority());
			assertEquals(wm.isSuccess(), rm.isSuccess());
			assertEquals(wm.getTopic(), rm.getTopic());
			assertEquals(wm.getProperties(), rm.getProperties());
		}

	}
}
