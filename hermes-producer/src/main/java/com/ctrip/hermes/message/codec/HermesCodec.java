package com.ctrip.hermes.message.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;

public class HermesCodec {

	private ByteArrayOutputStream m_out;

	private byte[] m_in;

	private int m_cursor;

	private byte[] writeBuffer = new byte[8];

	public HermesCodec(ByteArrayOutputStream out) {
		m_out = out;
	}

	public HermesCodec(byte[] in) {
		m_in = in;
		m_cursor = 0;
	}

	public void writeBoolean(boolean b) throws IOException {
		m_out.write(b ? 1 : 0);
	}

	public void writeBytes(byte[] bytes) throws IOException {
		if (bytes == null || bytes.length == 0) {
			writeInt(0);
		} else {
			writeInt(bytes.length);
			m_out.write(bytes);
		}
	}

	public void writeString(String str) throws IOException {
		if (Strings.isNullOrEmpty(str)) {
			writeInt(0);
		} else {
			byte[] bytes = str.getBytes(Charsets.UTF_8);
			writeInt(bytes.length);
			m_out.write(bytes);
		}
	}

	public void writeInt(int i) {
		m_out.write((i >>> 24) & 0xFF);
		m_out.write((i >>> 16) & 0xFF);
		m_out.write((i >>> 8) & 0xFF);
		m_out.write((i >>> 0) & 0xFF);
	}

	private void advance(int i) {
		m_cursor += i;
	}

	public boolean readBoolean() {
		boolean result = m_in[m_cursor] == 1;
		advance(1);
		return result;
	}

	public int readInt() {
		int result = ((m_in[m_cursor] << 24) + (m_in[m_cursor + 1] << 16) + (m_in[m_cursor + 2] << 8) + (m_in[m_cursor + 3] << 0));
		advance(4);
		return result;
	}

	public String readString() {
		String result = null;
		int keyLen = readInt();

		if (keyLen > 0) {
			result = new String(m_in, m_cursor, keyLen);
			m_cursor += keyLen;
		}

		return result;
	}

	public byte[] readBytes() {
		byte[] result = null;

		int bodyLen = readInt();
		if (bodyLen > 0) {
			result = new byte[bodyLen];
			System.arraycopy(m_in, m_cursor, result, 0, bodyLen);
			advance(bodyLen);
		}

		return result;
	}

	public void writeLong(long v) {
		writeBuffer[0] = (byte) (v >>> 56);
		writeBuffer[1] = (byte) (v >>> 48);
		writeBuffer[2] = (byte) (v >>> 40);
		writeBuffer[3] = (byte) (v >>> 32);
		writeBuffer[4] = (byte) (v >>> 24);
		writeBuffer[5] = (byte) (v >>> 16);
		writeBuffer[6] = (byte) (v >>> 8);
		writeBuffer[7] = (byte) (v >>> 0);
		m_out.write(writeBuffer, 0, 8);
	}

	public long readLong() {
		long result = (((long) m_in[m_cursor] << 56) + ((long) (m_in[m_cursor + 1] & 255) << 48)
		      + ((long) (m_in[m_cursor + 2] & 255) << 40) + ((long) (m_in[m_cursor + 3] & 255) << 32)
		      + ((long) (m_in[m_cursor + 4] & 255) << 24) + ((m_in[m_cursor + 5] & 255) << 16)
		      + ((m_in[m_cursor + 6] & 255) << 8) + ((m_in[m_cursor + 7] & 255) << 0));

		advance(8);

		return result;
	}

	public void writeNull() {
		// TODO should add type info before every type
		writeInt(Integer.MIN_VALUE);
	}

	public boolean nextNull() {
		int nextInt = readInt();
		if (nextInt == Integer.MIN_VALUE) {
			return true;
		} else {
			advance(-4);
			return false;
		}

	}

}
