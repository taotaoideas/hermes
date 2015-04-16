package com.ctrip.hermes.example.performance;

import java.util.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.dal.hermes.DeadLetter;
import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.core.bo.Tpp;

/**
 * Test the performance on: broker write data to storage
 * Assume that network is not th bottleneck.
 */
public class BrokerToStorage extends ComponentTestCase {
//
//	private String topic = "order_new";
//
//	private int partition = 0;
//	private MessageService s;
//
//	@Before
//	public void before() {
//		s = lookup(MessageService.class);
//	}
//
//	@BeforeClass
//	public static void beforeClass() {
//		System.setProperty("devMode", "false");
//	}
//
//	@Test
//	public void testMultiWrite() throws DalException {
//		final int count = 10000;
//		Tpp tpp = new Tpp(topic, partition, true);
//
//		List<MessagePriority> wmsgs = makeMessages(tpp, count);
//
//		// output the insert sql:
//		StringBuilder sb = new StringBuilder();
//		sb.append("INSERT INTO 100_0.message_0(producer_ip,producer_id,ref_key,attributes,creation_date,payload) " +
//				  "VALUES");
//
//		for (MessagePriority msg : wmsgs) {
//			sb.append("('");
//			sb.append(msg.getProducerIp()); sb.append("', '");
//			sb.append(msg.getProducerId()); sb.append("', '");
//			sb.append(msg.getRefKey()); sb.append("', '");
//			sb.append(msg.getAttributes()); sb.append("', '");
//			sb.append("2015-04-13 16:28:34"); sb.append("', '");
//			sb.append(msg.getPayload()); sb.append("'),");
//		}
//		sb.deleteCharAt(sb.length()-1);
////		System.out.println("\n\n " + sb.toString() + "\n");
//
//
//
//		long startTime = System.currentTimeMillis();
//		s.write(wmsgs);
//		long endTime = System.currentTimeMillis();
//
//		System.out.println(String.format("Write %d msgs in %.2f(s). QPS: %.2f msg/s", count, (endTime - startTime)/1000f,
//				  count/((endTime - startTime)/1000f)));
//	}
//
//	public static List<MessagePriority> makeMessages(Tpp tpp, int count) {
//		List<MessagePriority> result = new ArrayList<>();
//
//		for (int i = 0; i < count; i++) {
//			result.add(makeMessage(tpp));
//		}
//
//		return result;
//	}
//
//	public static MessagePriority makeMessage(Tpp tpp) {
//		MessagePriority m = new MessagePriority();
//		Random rnd = new Random();
//
//		String attributes = uuid();
//		Date creationDate = new Date();
//		byte[] payload = uuid().getBytes();
//		int producerId = rnd.nextInt(1000);
//		String producerIp = uuid().substring(0, 10);
//
//		m.setAttributes(attributes);
//		m.setCreationDate(creationDate);
//		m.setPayload(payload);
//		m.setPriority(tpp.getPriorityInt());
//		m.setProducerId(producerId);
//		m.setProducerIp(producerIp);
//		m.setRefKey(uuid());
//		m.setPartition(tpp.getPartition());
//		m.setTopic(tpp.getTopic());
//		return m;
//	}
//
//	public static String uuid() {
//		return UUID.randomUUID().toString();
//	}
}
