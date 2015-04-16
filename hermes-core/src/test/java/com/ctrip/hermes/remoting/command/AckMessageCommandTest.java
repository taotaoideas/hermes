package com.ctrip.hermes.remoting.command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.AckMessageCommand;
import com.ctrip.hermes.core.transport.command.Header;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class AckMessageCommandTest {
	@Test
	public void test() {
		AckMessageCommand cmd = new AckMessageCommand();
		// ack start
		// t1
		cmd.addAckMsg(new Tpp("t1", 1, true), "g1", false, 1, 0);
		cmd.addAckMsg(new Tpp("t1", 1, true), "g1", false, 2, 0);

		cmd.addAckMsg(new Tpp("t1", 1, false), "g1", false, 1, 0);
		cmd.addAckMsg(new Tpp("t1", 1, false), "g1", false, 2, 0);
		cmd.addAckMsg(new Tpp("t1", 1, false), "g1", false, 3, 0);

		cmd.addAckMsg(new Tpp("t1", 2, true), "g1", false, 1, 0);
		cmd.addAckMsg(new Tpp("t1", 2, true), "g1", false, 2, 0);
		cmd.addAckMsg(new Tpp("t1", 2, true), "g1", false, 3, 0);
		cmd.addAckMsg(new Tpp("t1", 2, true), "g1", false, 4, 0);

		// t2
		cmd.addAckMsg(new Tpp("t2", 1, true), "g2", true, 1, 1);
		cmd.addAckMsg(new Tpp("t2", 1, true), "g2", true, 2, 2);

		cmd.addAckMsg(new Tpp("t2", 1, false), "g2", true, 1, 1);
		cmd.addAckMsg(new Tpp("t2", 1, false), "g2", true, 2, 2);
		cmd.addAckMsg(new Tpp("t2", 1, false), "g2", true, 3, 3);

		cmd.addAckMsg(new Tpp("t2", 2, false), "g2", true, 1, 4);
		cmd.addAckMsg(new Tpp("t2", 2, false), "g2", true, 2, 3);
		cmd.addAckMsg(new Tpp("t2", 2, false), "g2", true, 3, 2);
		cmd.addAckMsg(new Tpp("t2", 2, false), "g2", true, 4, 1);
		// ack end

		// nack start
		// t1
		cmd.addNackMsg(new Tpp("t1", 1, true), "g1", false, 1, 0);
		cmd.addNackMsg(new Tpp("t1", 1, true), "g1", false, 2, 0);

		cmd.addNackMsg(new Tpp("t1", 1, false), "g1", false, 1, 0);
		cmd.addNackMsg(new Tpp("t1", 1, false), "g1", false, 2, 0);
		cmd.addNackMsg(new Tpp("t1", 1, false), "g1", false, 3, 0);

		cmd.addNackMsg(new Tpp("t1", 2, true), "g1", false, 1, 0);
		cmd.addNackMsg(new Tpp("t1", 2, true), "g1", false, 2, 0);
		cmd.addNackMsg(new Tpp("t1", 2, true), "g1", false, 3, 0);
		cmd.addNackMsg(new Tpp("t1", 2, true), "g1", false, 4, 0);

		// t2
		cmd.addNackMsg(new Tpp("t2", 1, true), "g2", true, 1, 1);
		cmd.addNackMsg(new Tpp("t2", 1, true), "g2", true, 2, 2);

		cmd.addNackMsg(new Tpp("t2", 1, false), "g2", true, 1, 1);
		cmd.addNackMsg(new Tpp("t2", 1, false), "g2", true, 2, 2);
		cmd.addNackMsg(new Tpp("t2", 1, false), "g2", true, 3, 3);

		cmd.addNackMsg(new Tpp("t2", 2, false), "g2", true, 1, 4);
		cmd.addNackMsg(new Tpp("t2", 2, false), "g2", true, 2, 3);
		cmd.addNackMsg(new Tpp("t2", 2, false), "g2", true, 3, 2);
		cmd.addNackMsg(new Tpp("t2", 2, false), "g2", true, 4, 1);
		// nack end

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		AckMessageCommand decodedCmd = new AckMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> nackMsgs = decodedCmd.getNackMsgs();

		// acks
		Assert.assertEquals(6, ackMsgs.size());

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).keySet(), Arrays.asList(1L, 2L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).values(), Arrays.asList(0, 0));

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).keySet(), Arrays.asList(1L, 2L, 3L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).values(), Arrays.asList(0, 0, 0));

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).keySet(),
		      Arrays.asList(1L, 2L, 3L, 4L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).values(), Arrays.asList(0, 0, 0, 0));

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).keySet(), Arrays.asList(1L, 2L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).values(), Arrays.asList(1, 2));

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).keySet(), Arrays.asList(1L, 2L, 3L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).values(), Arrays.asList(1, 2, 3));

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).keySet(),
		      Arrays.asList(1L, 2L, 3L, 4L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).values(), Arrays.asList(4, 3, 2, 1));

		// nacks
		Assert.assertEquals(6, nackMsgs.size());

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).keySet(), Arrays.asList(1L, 2L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).values(), Arrays.asList(0, 0));

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).keySet(), Arrays.asList(1L, 2L, 3L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).values(), Arrays.asList(0, 0, 0));

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).keySet(),
		      Arrays.asList(1L, 2L, 3L, 4L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).values(), Arrays.asList(0, 0, 0, 0));

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).keySet(), Arrays.asList(1L, 2L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).values(), Arrays.asList(1, 2));

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).keySet(), Arrays.asList(1L, 2L, 3L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).values(), Arrays.asList(1, 2, 3));

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).keySet(),
		      Arrays.asList(1L, 2L, 3L, 4L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).values(), Arrays.asList(4, 3, 2, 1));

	}

	private <T> void assertEquals(Collection<T> c1, Collection<T> c2) {
		if (c1 == null && c2 != null) {
			Assert.fail();
		}

		if (c1 != null && c2 == null) {
			Assert.fail();
		}

		if (c1 != null && c2 != null) {
			Assert.assertEquals(c1.size(), c2.size());
			Iterator<T> it1 = c1.iterator();
			Iterator<T> it2 = c2.iterator();
			for (int i = 0; i < c1.size(); i++) {
				Assert.assertEquals(it1.next(), it2.next());
			}
		} else {
			Assert.fail();
		}
	}

	@Test
	public void testEmpty() {
		AckMessageCommand cmd = new AckMessageCommand();

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		AckMessageCommand decodedCmd = new AckMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> nackMsgs = decodedCmd.getNackMsgs();

		Assert.assertEquals(0, ackMsgs.size());

		Assert.assertEquals(0, nackMsgs.size());
	}

	@Test
	public void testAckEmpty() {
		AckMessageCommand cmd = new AckMessageCommand();

		// nack start
		// t1
		cmd.addNackMsg(new Tpp("t1", 1, true), "g1", false, 1, 0);
		cmd.addNackMsg(new Tpp("t1", 1, true), "g1", false, 2, 0);

		cmd.addNackMsg(new Tpp("t1", 1, false), "g1", false, 1, 0);
		cmd.addNackMsg(new Tpp("t1", 1, false), "g1", false, 2, 0);
		cmd.addNackMsg(new Tpp("t1", 1, false), "g1", false, 3, 0);

		cmd.addNackMsg(new Tpp("t1", 2, true), "g1", false, 1, 0);
		cmd.addNackMsg(new Tpp("t1", 2, true), "g1", false, 2, 0);
		cmd.addNackMsg(new Tpp("t1", 2, true), "g1", false, 3, 0);
		cmd.addNackMsg(new Tpp("t1", 2, true), "g1", false, 4, 0);

		// t2
		cmd.addNackMsg(new Tpp("t2", 1, true), "g2", true, 1, 1);
		cmd.addNackMsg(new Tpp("t2", 1, true), "g2", true, 2, 2);

		cmd.addNackMsg(new Tpp("t2", 1, false), "g2", true, 1, 1);
		cmd.addNackMsg(new Tpp("t2", 1, false), "g2", true, 2, 2);
		cmd.addNackMsg(new Tpp("t2", 1, false), "g2", true, 3, 3);

		cmd.addNackMsg(new Tpp("t2", 2, false), "g2", true, 1, 4);
		cmd.addNackMsg(new Tpp("t2", 2, false), "g2", true, 2, 3);
		cmd.addNackMsg(new Tpp("t2", 2, false), "g2", true, 3, 2);
		cmd.addNackMsg(new Tpp("t2", 2, false), "g2", true, 4, 1);
		// nack end

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		AckMessageCommand decodedCmd = new AckMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> nackMsgs = decodedCmd.getNackMsgs();

		// acks
		Assert.assertEquals(0, ackMsgs.size());

		// nacks
		Assert.assertEquals(6, nackMsgs.size());

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).keySet(), Arrays.asList(1L, 2L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).values(), Arrays.asList(0, 0));

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).keySet(), Arrays.asList(1L, 2L, 3L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).values(), Arrays.asList(0, 0, 0));

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).keySet(),
		      Arrays.asList(1L, 2L, 3L, 4L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).values(), Arrays.asList(0, 0, 0, 0));

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).keySet(), Arrays.asList(1L, 2L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).values(), Arrays.asList(1, 2));

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).keySet(), Arrays.asList(1L, 2L, 3L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).values(), Arrays.asList(1, 2, 3));

		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).keySet(),
		      Arrays.asList(1L, 2L, 3L, 4L));
		assertEquals(nackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).values(), Arrays.asList(4, 3, 2, 1));

	}

	@Test
	public void testNackEmpty() {
		AckMessageCommand cmd = new AckMessageCommand();
		// ack start
		// t1
		cmd.addAckMsg(new Tpp("t1", 1, true), "g1", false, 1, 0);
		cmd.addAckMsg(new Tpp("t1", 1, true), "g1", false, 2, 0);

		cmd.addAckMsg(new Tpp("t1", 1, false), "g1", false, 1, 0);
		cmd.addAckMsg(new Tpp("t1", 1, false), "g1", false, 2, 0);
		cmd.addAckMsg(new Tpp("t1", 1, false), "g1", false, 3, 0);

		cmd.addAckMsg(new Tpp("t1", 2, true), "g1", false, 1, 0);
		cmd.addAckMsg(new Tpp("t1", 2, true), "g1", false, 2, 0);
		cmd.addAckMsg(new Tpp("t1", 2, true), "g1", false, 3, 0);
		cmd.addAckMsg(new Tpp("t1", 2, true), "g1", false, 4, 0);

		// t2
		cmd.addAckMsg(new Tpp("t2", 1, true), "g2", true, 1, 1);
		cmd.addAckMsg(new Tpp("t2", 1, true), "g2", true, 2, 2);

		cmd.addAckMsg(new Tpp("t2", 1, false), "g2", true, 1, 1);
		cmd.addAckMsg(new Tpp("t2", 1, false), "g2", true, 2, 2);
		cmd.addAckMsg(new Tpp("t2", 1, false), "g2", true, 3, 3);

		cmd.addAckMsg(new Tpp("t2", 2, false), "g2", true, 1, 4);
		cmd.addAckMsg(new Tpp("t2", 2, false), "g2", true, 2, 3);
		cmd.addAckMsg(new Tpp("t2", 2, false), "g2", true, 3, 2);
		cmd.addAckMsg(new Tpp("t2", 2, false), "g2", true, 4, 1);
		// ack end

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		AckMessageCommand decodedCmd = new AckMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> nackMsgs = decodedCmd.getNackMsgs();

		// acks
		Assert.assertEquals(6, ackMsgs.size());

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).keySet(), Arrays.asList(1L, 2L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).values(), Arrays.asList(0, 0));

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).keySet(), Arrays.asList(1L, 2L, 3L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).values(), Arrays.asList(0, 0, 0));

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).keySet(),
		      Arrays.asList(1L, 2L, 3L, 4L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).values(), Arrays.asList(0, 0, 0, 0));

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).keySet(), Arrays.asList(1L, 2L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).values(), Arrays.asList(1, 2));

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).keySet(), Arrays.asList(1L, 2L, 3L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).values(), Arrays.asList(1, 2, 3));

		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).keySet(),
		      Arrays.asList(1L, 2L, 3L, 4L));
		assertEquals(ackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).values(), Arrays.asList(4, 3, 2, 1));

		// nacks
		Assert.assertEquals(0, nackMsgs.size());

	}
}
