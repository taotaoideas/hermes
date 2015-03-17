package com.ctrip.hermes.broker.storage.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;

import com.ctrip.hermes.storage.pair.ClusteredMessagePair;
import com.ctrip.hermes.storage.pair.MessagePair;
import com.ctrip.hermes.storage.pair.ResendPair;
import com.mysql.jdbc.Driver;

public class MysqlGroup {

	private Connection m_conn;

	private String m_topic;

	private String m_groupId;

	public MysqlGroup(String topic, String groupId) {
		m_topic = topic;
		m_groupId = groupId;

		try {
			Driver.class.newInstance();
			m_conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1/hermes", "root", null);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public ClusteredMessagePair createMessagePair() {
		MysqlMessageStorage main = new MysqlMessageStorage(m_conn, "msg_0_" + m_topic);

		String offsetTable = "msg_offset_" + m_topic;
		MysqlOffsetStorage offset = new MysqlOffsetStorage(m_conn, offsetTable, m_groupId);

		MessagePair pair = new MessagePair(main, offset);

		return new ClusteredMessagePair(Arrays.asList(pair));
	}

	public ResendPair createResendPair() {
		MysqlResendStorage main = new MysqlResendStorage(m_conn, "resend_" + m_topic + "_" + m_groupId, m_topic);
		MysqlOffsetStorage offset = new MysqlOffsetStorage(m_conn, "resend_offset_" + m_topic, m_groupId);
		ResendPair pair = new ResendPair(main, offset, Long.MAX_VALUE);
		return pair;
	}

}
