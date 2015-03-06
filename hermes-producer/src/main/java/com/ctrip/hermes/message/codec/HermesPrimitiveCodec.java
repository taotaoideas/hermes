package com.ctrip.hermes.message.codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;

public class HermesPrimitiveCodec {

	private ByteBuffer m_buf;

	public HermesPrimitiveCodec(ByteBuffer buf) {
		m_buf = buf;
		m_buf.order(ByteOrder.BIG_ENDIAN);
	}

	public void writeBoolean(boolean b) {
		m_buf.put(b ? (byte) 1 : (byte) 0);
	}

	public void writeBytes(byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			writeInt(0);
		} else {
			writeInt(bytes.length);
			m_buf.put(bytes);
		}
	}

	public void writeString(String str) {
		if (Strings.isNullOrEmpty(str)) {
			writeInt(0);
		} else {
			byte[] bytes = str.getBytes(Charsets.UTF_8);
			writeInt(bytes.length);
			m_buf.put(bytes);
		}
	}

	public void writeInt(int i) {
		m_buf.putInt(i);
	}

	public boolean readBoolean() {
		return m_buf.get() == 1;
	}

	public int readInt() {
		return m_buf.getInt();
	}

	public String readString() {
		String result = null;
		int strLen = readInt();

		if (strLen > 0) {
			byte[] strBytes = new byte[strLen];
			m_buf.get(strBytes);
			result = new String(strBytes, Charsets.UTF_8);
		}

		return result;
	}

	public byte[] readBytes() {
		byte[] result = null;

		int bodyLen = readInt();
		if (bodyLen > 0) {
			result = new byte[bodyLen];
			m_buf.get(result);
		}

		return result;
	}

	public void writeLong(long v) {
		m_buf.putLong(v);
	}

	public long readLong() {
		return m_buf.getLong();
	}

	public void writeNull() {
		// TODO should add type info before every type
		writeInt(Integer.MIN_VALUE);
	}

	public boolean nextNull() {
		m_buf.mark();
		int nextInt = readInt();
		if (nextInt == Integer.MIN_VALUE) {
			return true;
		} else {
			m_buf.reset();
			return false;
		}

	}

	public ByteBuffer getBuf() {
		return m_buf;
	}

	public void writeObject(Object value) {
		// TODO
		if (value == null) {
			m_buf.put((byte) 0);
		} else if (value instanceof String) {
			m_buf.put((byte) 1);
			writeString((String) value);
		} else if (value instanceof Long) {
			m_buf.put((byte) 2);
			writeLong((Long) value);
		} else if (value instanceof Integer) {
			m_buf.put((byte) 3);
			writeInt((Integer) value);
		} else {
			throw new RuntimeException("Unsupported value type");
		}
	}

	public Object readObject() {
		int type = m_buf.get();

		switch (type) {
		case 0:
			return null;

		case 1:
			return readString();

		case 2:
			return readLong();

		case 3:
			return readInt();

		default:
			throw new RuntimeException("Unsupported value type");
		}

	}

}
