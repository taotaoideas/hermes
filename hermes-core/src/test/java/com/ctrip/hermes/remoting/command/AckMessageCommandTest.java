package com.ctrip.hermes.remoting.command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.Set;
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
		cmd.addAckMsg("t1", 1, true, "g1", false, 1);
		cmd.addAckMsg("t1", 1, true, "g1", false, 2);

		cmd.addAckMsg("t1", 1, false, "g1", false, 1);
		cmd.addAckMsg("t1", 1, false, "g1", false, 2);
		cmd.addAckMsg("t1", 1, false, "g1", false, 3);

		cmd.addAckMsg("t1", 2, true, "g1", false, 1);
		cmd.addAckMsg("t1", 2, true, "g1", false, 2);
		cmd.addAckMsg("t1", 2, true, "g1", false, 3);
		cmd.addAckMsg("t1", 2, true, "g1", false, 4);

		// t2
		cmd.addAckMsg("t2", 1, true, "g2", true, 1);
		cmd.addAckMsg("t2", 1, true, "g2", true, 2);

		cmd.addAckMsg("t2", 1, false, "g2", true, 1);
		cmd.addAckMsg("t2", 1, false, "g2", true, 2);
		cmd.addAckMsg("t2", 1, false, "g2", true, 3);

		cmd.addAckMsg("t2", 2, false, "g2", true, 1);
		cmd.addAckMsg("t2", 2, false, "g2", true, 2);
		cmd.addAckMsg("t2", 2, false, "g2", true, 3);
		cmd.addAckMsg("t2", 2, false, "g2", true, 4);
		// ack end

		// nack start
		// t1
		cmd.addNackMsg("t1", 1, true, "g1", false, 1);
		cmd.addNackMsg("t1", 1, true, "g1", false, 2);

		cmd.addNackMsg("t1", 1, false, "g1", false, 1);
		cmd.addNackMsg("t1", 1, false, "g1", false, 2);
		cmd.addNackMsg("t1", 1, false, "g1", false, 3);

		cmd.addNackMsg("t1", 2, true, "g1", false, 1);
		cmd.addNackMsg("t1", 2, true, "g1", false, 2);
		cmd.addNackMsg("t1", 2, true, "g1", false, 3);
		cmd.addNackMsg("t1", 2, true, "g1", false, 4);

		// t2
		cmd.addNackMsg("t2", 1, true, "g2", true, 1);
		cmd.addNackMsg("t2", 1, true, "g2", true, 2);

		cmd.addNackMsg("t2", 1, false, "g2", true, 1);
		cmd.addNackMsg("t2", 1, false, "g2", true, 2);
		cmd.addNackMsg("t2", 1, false, "g2", true, 3);

		cmd.addNackMsg("t2", 2, false, "g2", true, 1);
		cmd.addNackMsg("t2", 2, false, "g2", true, 2);
		cmd.addNackMsg("t2", 2, false, "g2", true, 3);
		cmd.addNackMsg("t2", 2, false, "g2", true, 4);
		// nack end

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		AckMessageCommand decodedCmd = new AckMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> nackMsgs = decodedCmd.getNackMsgs();

		Assert.assertEquals(6, ackMsgs.size());

		ackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).containsAll(Arrays.asList(1L, 2L));
		ackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).containsAll(Arrays.asList(1L, 2L, 3L));
		ackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
		ackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).containsAll(Arrays.asList(1L, 2L));
		ackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).containsAll(Arrays.asList(1L, 2L, 3L));
		ackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));

		Assert.assertEquals(6, nackMsgs.size());
		nackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).containsAll(Arrays.asList(1L, 2L));
		nackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).containsAll(Arrays.asList(1L, 2L, 3L));
		nackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
		nackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).containsAll(Arrays.asList(1L, 2L));
		nackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).containsAll(Arrays.asList(1L, 2L, 3L));
		nackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
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

		ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> nackMsgs = decodedCmd.getNackMsgs();

		Assert.assertEquals(0, ackMsgs.size());

		Assert.assertEquals(0, nackMsgs.size());
	}

	@Test
	public void testAckEmpty() {
		AckMessageCommand cmd = new AckMessageCommand();

		// nack start
		// t1
		cmd.addNackMsg("t1", 1, true, "g1", false, 1);
		cmd.addNackMsg("t1", 1, true, "g1", false, 2);

		cmd.addNackMsg("t1", 1, false, "g1", false, 1);
		cmd.addNackMsg("t1", 1, false, "g1", false, 2);
		cmd.addNackMsg("t1", 1, false, "g1", false, 3);

		cmd.addNackMsg("t1", 2, true, "g1", false, 1);
		cmd.addNackMsg("t1", 2, true, "g1", false, 2);
		cmd.addNackMsg("t1", 2, true, "g1", false, 3);
		cmd.addNackMsg("t1", 2, true, "g1", false, 4);

		// t2
		cmd.addNackMsg("t2", 1, true, "g2", true, 1);
		cmd.addNackMsg("t2", 1, true, "g2", true, 2);

		cmd.addNackMsg("t2", 1, false, "g2", true, 1);
		cmd.addNackMsg("t2", 1, false, "g2", true, 2);
		cmd.addNackMsg("t2", 1, false, "g2", true, 3);

		cmd.addNackMsg("t2", 2, false, "g2", true, 1);
		cmd.addNackMsg("t2", 2, false, "g2", true, 2);
		cmd.addNackMsg("t2", 2, false, "g2", true, 3);
		cmd.addNackMsg("t2", 2, false, "g2", true, 4);
		// nack end

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		AckMessageCommand decodedCmd = new AckMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> nackMsgs = decodedCmd.getNackMsgs();

		Assert.assertEquals(0, ackMsgs.size());

		Assert.assertEquals(6, nackMsgs.size());
		nackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).containsAll(Arrays.asList(1L, 2L));
		nackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).containsAll(Arrays.asList(1L, 2L, 3L));
		nackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
		nackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).containsAll(Arrays.asList(1L, 2L));
		nackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).containsAll(Arrays.asList(1L, 2L, 3L));
		nackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
	}

	@Test
	public void testNackEmpty() {
		AckMessageCommand cmd = new AckMessageCommand();
		// ack start
		// t1
		cmd.addAckMsg("t1", 1, true, "g1", false, 1);
		cmd.addAckMsg("t1", 1, true, "g1", false, 2);

		cmd.addAckMsg("t1", 1, false, "g1", false, 1);
		cmd.addAckMsg("t1", 1, false, "g1", false, 2);
		cmd.addAckMsg("t1", 1, false, "g1", false, 3);

		cmd.addAckMsg("t1", 2, true, "g1", false, 1);
		cmd.addAckMsg("t1", 2, true, "g1", false, 2);
		cmd.addAckMsg("t1", 2, true, "g1", false, 3);
		cmd.addAckMsg("t1", 2, true, "g1", false, 4);

		// t2
		cmd.addAckMsg("t2", 1, true, "g2", true, 1);
		cmd.addAckMsg("t2", 1, true, "g2", true, 2);

		cmd.addAckMsg("t2", 1, false, "g2", true, 1);
		cmd.addAckMsg("t2", 1, false, "g2", true, 2);
		cmd.addAckMsg("t2", 1, false, "g2", true, 3);

		cmd.addAckMsg("t2", 2, false, "g2", true, 1);
		cmd.addAckMsg("t2", 2, false, "g2", true, 2);
		cmd.addAckMsg("t2", 2, false, "g2", true, 3);
		cmd.addAckMsg("t2", 2, false, "g2", true, 4);
		// ack end

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		AckMessageCommand decodedCmd = new AckMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> nackMsgs = decodedCmd.getNackMsgs();

		Assert.assertEquals(6, ackMsgs.size());

		ackMsgs.get(new Triple<>(new Tpp("t1", 1, true), "g1", false)).containsAll(Arrays.asList(1L, 2L));
		ackMsgs.get(new Triple<>(new Tpp("t1", 1, false), "g1", false)).containsAll(Arrays.asList(1L, 2L, 3L));
		ackMsgs.get(new Triple<>(new Tpp("t1", 2, true), "g1", false)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
		ackMsgs.get(new Triple<>(new Tpp("t2", 1, true), "g2", true)).containsAll(Arrays.asList(1L, 2L));
		ackMsgs.get(new Triple<>(new Tpp("t2", 1, false), "g2", true)).containsAll(Arrays.asList(1L, 2L, 3L));
		ackMsgs.get(new Triple<>(new Tpp("t2", 2, false), "g2", true)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));

		Assert.assertEquals(0, nackMsgs.size());
	}
}
