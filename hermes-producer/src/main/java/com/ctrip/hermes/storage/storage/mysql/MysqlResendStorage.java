package com.ctrip.hermes.storage.storage.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.spi.typed.ResendStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.ContinuousRange;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;
import com.ctrip.hermes.storage.util.CollectionUtil;

public class MysqlResendStorage implements ResendStorage {

	private String m_table;

	private Connection m_conn;

	public MysqlResendStorage(Connection conn, String table) {
		m_conn = conn;
		m_table = table;
	}

	@Override
	public void append(List<Resend> resends) throws StorageException {
		try {
			doAppend(resends);
		} catch (SQLException e) {
			throw new StorageException("", e);
		}
	}

	private void doAppend(List<Resend> resends) throws SQLException {
		String sql = String.format("insert into %s (start, end, sid) values (?,?,?)", m_table);
		PreparedStatement stmt = m_conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		for (Resend resend : resends) {
			Range r = resend.getRange();
			stmt.setLong(1, r.getStartOffset().getOffset());
			stmt.setLong(2, r.getEndOffset().getOffset());
			stmt.setString(3, r.getId());

			stmt.addBatch();
		}

		stmt.executeBatch();

		ResultSet rs = stmt.getGeneratedKeys();
		int i = 0;
		while (rs.next()) {
			resends.get(i).getRange().setOffset(new Offset(m_table, rs.getLong(1)));
		}
	}

	private List<Resend> rsToResend(ResultSet rs) throws SQLException {
		List<Resend> result = new ArrayList<Resend>();

		while (rs.next()) {
			String id = rs.getString(4);
			Offset storageOffset = new Offset(id, rs.getLong(1));
			Offset start = new Offset(id, rs.getLong(2));
			Offset end = new Offset(id, rs.getLong(3));
			ContinuousRange r = new ContinuousRange(start, end);
			r.setOffset(storageOffset);

			Resend resend = new Resend(r, rs.getLong(4));

			result.add(resend);
		}

		return result;
	}

	@Override
	public Browser<Resend> createBrowser(final long offset) {
		return new Browser<Resend>() {

			private long m_offset = offset;

			@Override
			public List<Resend> read(int batchSize) throws Exception {
				String sql = String.format("select id,start,end,sid,due from %s where id >= ? order by id asc limit ?",
				      m_table);
				PreparedStatement stmt = m_conn.prepareStatement(sql);
				stmt.setLong(1, m_offset);
				stmt.setInt(2, batchSize);

				List<Resend> resends = rsToResend(stmt.executeQuery());
				updateOffset(resends);

				return resends;

			}

			private void updateOffset(List<Resend> resends) {
				if (CollectionUtil.notEmpty(resends)) {
					m_offset = CollectionUtil.last(resends).getRange().getOffset().getOffset() + 1;
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

	@Override
	public List<Resend> read(Range range) throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		return m_table;
	}

	@Override
	public Resend top() {
		// TODO
		throw new UnsupportedOperationException();
	}

}
