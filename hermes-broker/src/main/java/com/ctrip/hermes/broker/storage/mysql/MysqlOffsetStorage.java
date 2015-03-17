package com.ctrip.hermes.broker.storage.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.ctrip.hermes.storage.spi.typed.OffsetStorage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public class MysqlOffsetStorage implements OffsetStorage {

	private Connection m_conn;

	private String m_table;

	private String m_groupId;

	public MysqlOffsetStorage(Connection conn, String table, String groupId) {
		m_conn = conn;
		m_table = table;
		m_groupId = groupId;
	}

	@Override
	public void append(List<Offset> offsets) throws StorageException {
		try {
			doAppend(offsets);
		} catch (Exception e) {
			throw new StorageException("", e);
		}
	}

	private void doAppend(List<Offset> offsets) throws Exception {
		String sql = String.format( //
		      "insert into %s (group_id, target, offset)" //
		            + " values (?,?,?)" //
		            + " on duplicate key update offset=?", m_table);
		PreparedStatement stmt = m_conn.prepareStatement(sql);
		for (Offset o : offsets) {
			stmt.setString(1, m_groupId);
			stmt.setString(2, o.getId());
			stmt.setLong(3, o.getOffset());
			stmt.setLong(4, o.getOffset());
			stmt.addBatch();
		}

		stmt.executeUpdate();
	}

	@Override
	public Browser<Offset> createBrowser(long offset) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Offset> read(Range range) throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		return m_table;
	}

	@Override
	public Offset top() throws StorageException {
		try {
			return new Offset(m_table, doTop());
		} catch (Exception e) {
			throw new StorageException("", e);
		}

	}

	private long doTop() throws SQLException {
		String sql = String.format("select offset from %s order by id desc limit 1", m_table);
		PreparedStatement stmt = m_conn.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();

		long offset = Offset.OLDEST;

		if (rs.next()) {
			offset = rs.getLong(1);
		}

		return offset;
	}

}
