package com.ctrip.hermes.broker;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.dal.MessageUtil;
import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.core.bo.Tpp;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class MessageDaoTest extends ComponentTestCase {

	@BeforeClass
	public static void bc() {
		System.setProperty("devMode", "true");
	}

	@Test
	public void testRaw() throws Exception {
		MysqlDataSource ds = new MysqlDataSource();
		ds.setUrl("jdbc:mysql://127.0.0.1/100_0?rewriteBatchedStatements=true");
		// ds.setUrl("jdbc:mysql://127.0.0.1/100_0");
		ds.setUser("root");
		ds.setPassword("123456");

		String sql = "INSERT INTO `message_0` "
		      + "(`producer_ip`, `producer_id`, `ref_key`, `attributes`, `creation_date`, `payload`)"
		      + " VALUES (?,?,?,?,?,?)";
		Connection conn = ds.getConnection();
		conn.setAutoCommit(true);
		PreparedStatement stmt = conn.prepareStatement(sql);
		//
		// for (int i = 0; i < 5; i++) {
		// stmt.setString(1, "xx");
		// stmt.setLong(2, 1);
		// stmt.setString(3, "xx");
		// stmt.setString(4, "xx");
		// stmt.setDate(5, new Date(System.currentTimeMillis()));
		// stmt.setBlob(6, new ByteArrayInputStream("hello".getBytes()));
		//
		// stmt.addBatch();
		// }
		// stmt.executeBatch();
		// conn.commit();
		// stmt.clearBatch();

		for (int i = 0; i < 6000; i++) {
			stmt.setString(1, "xx");
			stmt.setLong(2, 1);
			stmt.setString(3, "xx");
			stmt.setString(4, "xx");
			stmt.setDate(5, new Date(System.currentTimeMillis()));
			ByteArrayInputStream in = new ByteArrayInputStream("xx".getBytes());
			stmt.setBlob(6, in, in.available());

			stmt.addBatch();
		}

		long start = System.currentTimeMillis();
		int[] xx = stmt.executeBatch();
		System.out.println(xx.length);
//		conn.commit();
		System.out.println(System.currentTimeMillis() - start);

	}

	@Test
	public void test() throws Exception {
		MessagePriorityDao dao = lookup(MessagePriorityDao.class);

		MessagePriority[] msgs = new MessagePriority[6000];
		Tpp tpp = new Tpp("order_new", 0, true);
		for (int i = 0; i < msgs.length; i++) {
			msgs[i] = MessageUtil.makeMessage(tpp);
		}

		dao.insert(MessageUtil.makeMessage(tpp));

		long start = System.currentTimeMillis();
		dao.insert(msgs);
		System.out.println(System.currentTimeMillis() - start);
	}

}
