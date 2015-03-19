package com.ctrip.hermes.message.codec;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.message.ProducerMessage;

public class DefaultMessageCodecTest extends ComponentTestCase {

	@Test
	public void test() {
		MessageCodec msgCodec = lookup(MessageCodec.class);

		Pojo writePojo = new Pojo(UUID.randomUUID().toString(), 99);
		ProducerMessage<Pojo> writeMsg = new ProducerMessage<>();
		writeMsg.setBody(writePojo);
		writeMsg.setKey(UUID.randomUUID().toString());
		writeMsg.setPartition(UUID.randomUUID().toString());
		writeMsg.setTopic(UUID.randomUUID().toString());
		writeMsg.setPriority(true);
		writeMsg.addProperty(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		writeMsg.addProperty(UUID.randomUUID().toString(), UUID.randomUUID().toString());

		ByteBuffer buf = msgCodec.encode(writeMsg);
		buf.flip();
		
		ProducerMessage<byte[]> readMsg = msgCodec.decode(buf);

		assertEquals(writeMsg.getKey(), readMsg.getKey());
		assertEquals(writeMsg.getPartition(), readMsg.getPartition());
		assertEquals(writeMsg.getTopic(), readMsg.getTopic());
		assertEquals(writeMsg.isPriority(), readMsg.isPriority());
		assertEquals(writeMsg.getProperties(), readMsg.getProperties());

		Codec codec = lookup(CodecManager.class).getCodec(writeMsg.getTopic());
		Pojo readPojo = codec.decode(readMsg.getBody(), Pojo.class);

		assertEquals(writePojo, readPojo);
	}

	public static class Pojo {
		private String name;

		private int age;

		public Pojo() {
		}

		public Pojo(String name, int age) {
			this.name = name;
			this.age = age;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + age;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Pojo other = (Pojo) obj;
			if (age != other.age)
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}

	}

}
