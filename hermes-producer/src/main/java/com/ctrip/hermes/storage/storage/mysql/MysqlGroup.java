package com.ctrip.hermes.storage.storage.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;

import com.ctrip.hermes.storage.pair.ClusteredMessagePair;
import com.ctrip.hermes.storage.pair.MessagePair;
import com.ctrip.hermes.storage.pair.ResendPair;
import com.mysql.jdbc.Driver;

public class MysqlGroup {

    private Connection m_conn;
    private String m_uid;

    public MysqlGroup(String uid) {
        m_uid = uid;

        try {
            Driver.class.newInstance();
            m_conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1/mq", "root", null);
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public ClusteredMessagePair createMessagePairs() {
        MysqlMessageStorage main = new MysqlMessageStorage(m_conn, "msg");

        String offsetTable = "msg_offset_" + m_uid;
        MysqlOffsetStorage offset = new MysqlOffsetStorage(m_conn, offsetTable);

        MessagePair pair = new MessagePair(main, offset);

        return new ClusteredMessagePair(Arrays.asList(pair));
    }

    public ResendPair createResendPair() {
        MysqlResendStorage main = new MysqlResendStorage(m_conn, "resend_" + m_uid);
        MysqlOffsetStorage offset = new MysqlOffsetStorage(m_conn, "resend_offset_" + m_uid);
        ResendPair pair = new ResendPair(main, offset, Long.MAX_VALUE);
        return pair;
    }

}
