package com.ctrip.hermes.storage.storage.mysql;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.spi.typed.MessageStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;
import com.ctrip.hermes.storage.util.CollectionUtil;

public class MysqlMessageStorage implements MessageStorage {

	private Connection m_conn;

	private String m_id;

	public MysqlMessageStorage(Connection conn, String id) {
		m_conn = conn;
		m_id = id;
	}

	public void append(List<Record> msgs) throws StorageException {
		try {
			doAppend(msgs);
		} catch (Exception e) {
			throw new StorageException(e);
		}
	}

	private void doAppend(List<Record> msgs) throws IOException, SQLException {
		PreparedStatement stmt = m_conn.prepareStatement("insert into msg (c) values (?)",
		      Statement.RETURN_GENERATED_KEYS);
		for (Record m : msgs) {
			stmt.setBlob(1, new ByteArrayInputStream(m.getContent()));
			stmt.addBatch();
		}

		stmt.executeBatch();

		ResultSet rs = stmt.getGeneratedKeys();
		int i = 0;
		while (rs.next()) {
			msgs.get(i).setOffset(new Offset(m_id, rs.getLong(1)));
		}

	}

	private List<Record> rsToMessage(ResultSet rs) throws SQLException {
		List<Record> result = new ArrayList<Record>();

		while (rs.next()) {
			Record msg = new Record();
			msg.setOffset(new Offset(m_id, rs.getLong(1)));
			msg.setContent(rs.getBytes(2));

			result.add(msg);
		}

		return result;
	}

	public Browser<Record> createBrowser(final long offset) {

		return new Browser<Record>() {

			private long m_offset = offset;

			@Override
			public List<Record> read(int batchSize) throws Exception {
				PreparedStatement stmt = m_conn
				      .prepareStatement("select id,c from msg where id >= ? order by id asc limit ?");
				stmt.setLong(1, m_offset);
				stmt.setInt(2, batchSize);

				List<Record> msgs = rsToMessage(stmt.executeQuery());
				updateOffset(msgs);

				return msgs;

			}

			private void updateOffset(List<Record> msgs) {
				if (CollectionUtil.notEmpty(msgs)) {
					m_offset = msgs.get(msgs.size() - 1).getOffset().getOffset() + 1;
				}
			}

			@Override
			public void seek(long offset) {

			}

			@Override
			public long currentOffset() {
				return offset;
			}
		};
	}

	public List<Record> read(Range range) throws StorageException {
		String sqlTpl = "select id,c from msg where id  >= ? and id <= ?";
		String sql = String.format(sqlTpl, range.getStartOffset().getOffset(), range.getEndOffset().getOffset());

		try {
			ResultSet rs = m_conn.createStatement().executeQuery(sql);

			return rsToMessage(rs);
		} catch (Exception e) {
			throw new StorageException("", e);
		}
	}

	public String getId() {
		return m_id;
	}

	@Override
	public Record top() {
		// TODO
		throw new UnsupportedOperationException();
	}

}
