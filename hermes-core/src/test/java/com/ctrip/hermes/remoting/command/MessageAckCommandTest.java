package com.ctrip.hermes.remoting.command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.Header;
import com.ctrip.hermes.core.transport.command.MessageAckCommand;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class MessageAckCommandTest {
	@Test
	public void test() {
		MessageAckCommand cmd = new MessageAckCommand();
		// ack start
		// t1
		cmd.addAckMsg("t1", 1, true, 1);
		cmd.addAckMsg("t1", 1, true, 2);

		cmd.addAckMsg("t1", 1, false, 1);
		cmd.addAckMsg("t1", 1, false, 2);
		cmd.addAckMsg("t1", 1, false, 3);

		cmd.addAckMsg("t1", 2, true, 1);
		cmd.addAckMsg("t1", 2, true, 2);
		cmd.addAckMsg("t1", 2, true, 3);
		cmd.addAckMsg("t1", 2, true, 4);

		// t2
		cmd.addAckMsg("t2", 1, true, 1);
		cmd.addAckMsg("t2", 1, true, 2);

		cmd.addAckMsg("t2", 1, false, 1);
		cmd.addAckMsg("t2", 1, false, 2);
		cmd.addAckMsg("t2", 1, false, 3);

		cmd.addAckMsg("t2", 2, false, 1);
		cmd.addAckMsg("t2", 2, false, 2);
		cmd.addAckMsg("t2", 2, false, 3);
		cmd.addAckMsg("t2", 2, false, 4);
		// ack end

		// nack start
		// t1
		cmd.addNackMsg("t1", 1, true, 1);
		cmd.addNackMsg("t1", 1, true, 2);

		cmd.addNackMsg("t1", 1, false, 1);
		cmd.addNackMsg("t1", 1, false, 2);
		cmd.addNackMsg("t1", 1, false, 3);

		cmd.addNackMsg("t1", 2, true, 1);
		cmd.addNackMsg("t1", 2, true, 2);
		cmd.addNackMsg("t1", 2, true, 3);
		cmd.addNackMsg("t1", 2, true, 4);

		// t2
		cmd.addNackMsg("t2", 1, true, 1);
		cmd.addNackMsg("t2", 1, true, 2);

		cmd.addNackMsg("t2", 1, false, 1);
		cmd.addNackMsg("t2", 1, false, 2);
		cmd.addNackMsg("t2", 1, false, 3);

		cmd.addNackMsg("t2", 2, false, 1);
		cmd.addNackMsg("t2", 2, false, 2);
		cmd.addNackMsg("t2", 2, false, 3);
		cmd.addNackMsg("t2", 2, false, 4);
		// nack end

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		MessageAckCommand decodedCmd = new MessageAckCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Tpp, Set<Long>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Tpp, Set<Long>> nackMsgs = decodedCmd.getNackMsgs();

		Assert.assertEquals(6, ackMsgs.size());

		ackMsgs.get(new Tpp("t1", 1, true)).containsAll(Arrays.asList(1L, 2L));
		ackMsgs.get(new Tpp("t1", 1, false)).containsAll(Arrays.asList(1L, 2L, 3L));
		ackMsgs.get(new Tpp("t1", 2, true)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
		ackMsgs.get(new Tpp("t2", 1, true)).containsAll(Arrays.asList(1L, 2L));
		ackMsgs.get(new Tpp("t2", 1, false)).containsAll(Arrays.asList(1L, 2L, 3L));
		ackMsgs.get(new Tpp("t2", 2, false)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));

		Assert.assertEquals(6, nackMsgs.size());
		nackMsgs.get(new Tpp("t1", 1, true)).containsAll(Arrays.asList(1L, 2L));
		nackMsgs.get(new Tpp("t1", 1, false)).containsAll(Arrays.asList(1L, 2L, 3L));
		nackMsgs.get(new Tpp("t1", 2, true)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
		nackMsgs.get(new Tpp("t2", 1, true)).containsAll(Arrays.asList(1L, 2L));
		nackMsgs.get(new Tpp("t2", 1, false)).containsAll(Arrays.asList(1L, 2L, 3L));
		nackMsgs.get(new Tpp("t2", 2, false)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
	}

	@Test
	public void testEmpty() {
		MessageAckCommand cmd = new MessageAckCommand();

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		MessageAckCommand decodedCmd = new MessageAckCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Tpp, Set<Long>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Tpp, Set<Long>> nackMsgs = decodedCmd.getNackMsgs();

		Assert.assertEquals(0, ackMsgs.size());

		Assert.assertEquals(0, nackMsgs.size());
	}

	@Test
	public void testAckEmpty() {
		MessageAckCommand cmd = new MessageAckCommand();

		// nack start
		// t1
		cmd.addNackMsg("t1", 1, true, 1);
		cmd.addNackMsg("t1", 1, true, 2);

		cmd.addNackMsg("t1", 1, false, 1);
		cmd.addNackMsg("t1", 1, false, 2);
		cmd.addNackMsg("t1", 1, false, 3);

		cmd.addNackMsg("t1", 2, true, 1);
		cmd.addNackMsg("t1", 2, true, 2);
		cmd.addNackMsg("t1", 2, true, 3);
		cmd.addNackMsg("t1", 2, true, 4);

		// t2
		cmd.addNackMsg("t2", 1, true, 1);
		cmd.addNackMsg("t2", 1, true, 2);

		cmd.addNackMsg("t2", 1, false, 1);
		cmd.addNackMsg("t2", 1, false, 2);
		cmd.addNackMsg("t2", 1, false, 3);

		cmd.addNackMsg("t2", 2, false, 1);
		cmd.addNackMsg("t2", 2, false, 2);
		cmd.addNackMsg("t2", 2, false, 3);
		cmd.addNackMsg("t2", 2, false, 4);
		// nack end

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		MessageAckCommand decodedCmd = new MessageAckCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Tpp, Set<Long>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Tpp, Set<Long>> nackMsgs = decodedCmd.getNackMsgs();

		Assert.assertEquals(0, ackMsgs.size());

		Assert.assertEquals(6, nackMsgs.size());
		nackMsgs.get(new Tpp("t1", 1, true)).containsAll(Arrays.asList(1L, 2L));
		nackMsgs.get(new Tpp("t1", 1, false)).containsAll(Arrays.asList(1L, 2L, 3L));
		nackMsgs.get(new Tpp("t1", 2, true)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
		nackMsgs.get(new Tpp("t2", 1, true)).containsAll(Arrays.asList(1L, 2L));
		nackMsgs.get(new Tpp("t2", 1, false)).containsAll(Arrays.asList(1L, 2L, 3L));
		nackMsgs.get(new Tpp("t2", 2, false)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
	}

	@Test
	public void testNackEmpty() {
		MessageAckCommand cmd = new MessageAckCommand();
		// ack start
		// t1
		cmd.addAckMsg("t1", 1, true, 1);
		cmd.addAckMsg("t1", 1, true, 2);

		cmd.addAckMsg("t1", 1, false, 1);
		cmd.addAckMsg("t1", 1, false, 2);
		cmd.addAckMsg("t1", 1, false, 3);

		cmd.addAckMsg("t1", 2, true, 1);
		cmd.addAckMsg("t1", 2, true, 2);
		cmd.addAckMsg("t1", 2, true, 3);
		cmd.addAckMsg("t1", 2, true, 4);

		// t2
		cmd.addAckMsg("t2", 1, true, 1);
		cmd.addAckMsg("t2", 1, true, 2);

		cmd.addAckMsg("t2", 1, false, 1);
		cmd.addAckMsg("t2", 1, false, 2);
		cmd.addAckMsg("t2", 1, false, 3);

		cmd.addAckMsg("t2", 2, false, 1);
		cmd.addAckMsg("t2", 2, false, 2);
		cmd.addAckMsg("t2", 2, false, 3);
		cmd.addAckMsg("t2", 2, false, 4);
		// ack end

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		MessageAckCommand decodedCmd = new MessageAckCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Tpp, Set<Long>> ackMsgs = decodedCmd.getAckMsgs();
		ConcurrentMap<Tpp, Set<Long>> nackMsgs = decodedCmd.getNackMsgs();

		Assert.assertEquals(6, ackMsgs.size());

		ackMsgs.get(new Tpp("t1", 1, true)).containsAll(Arrays.asList(1L, 2L));
		ackMsgs.get(new Tpp("t1", 1, false)).containsAll(Arrays.asList(1L, 2L, 3L));
		ackMsgs.get(new Tpp("t1", 2, true)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));
		ackMsgs.get(new Tpp("t2", 1, true)).containsAll(Arrays.asList(1L, 2L));
		ackMsgs.get(new Tpp("t2", 1, false)).containsAll(Arrays.asList(1L, 2L, 3L));
		ackMsgs.get(new Tpp("t2", 2, false)).containsAll(Arrays.asList(1L, 2L, 3L, 4L));

		Assert.assertEquals(0, nackMsgs.size());
	}
}
