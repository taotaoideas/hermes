package com.ctrip.hermes.core.utils;

import io.netty.buffer.ByteBuf;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;

/**
 * Element Prefix +--------------------------------------------------------------+ Fixed Width: Following Octets null 0x01 0 OCTET
 * boolean 0x10(false) 0x11(true) 0 OCTET char 0x20 2 OCTET int 0x30 4 OCTET long 0x40 8 OCTET
 * +--------------------------------------------------------------+ Variable Width: Following Data Size Bytes 0x24 4 OCTET (max
 * size:2^32 = 2G) String 0x54 4 OCTET (max size:2^32 = 2G) +----------------------------------------------+---------------+ Array:
 * [count][Element][...] 0x64 followed 4 octet of data count Map: [count][[key Element][value Element]][...] 0x74 followed 4 octet
 * of data count
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class HermesPrimitiveCodec {

	private ByteBuf m_buf;

	public HermesPrimitiveCodec(ByteBuf buf) {
		m_buf = buf;
		m_buf.order(ByteOrder.BIG_ENDIAN);
	}

	public void writeBoolean(boolean b) {
		m_buf.writeByte(b ? Prefix.TRUE : Prefix.FALSE);
	}

	public boolean readBoolean() {
		byte type = m_buf.readByte();
		return readBoolean0(type);
	}

	private boolean readBoolean0(byte b) {
		return (b & 0x1) == 1;
	}

	public void writeBytes(byte[] bytes) {
		if (null == bytes) {
			writeNull();
		} else {
			m_buf.writeByte(Prefix.BYTES);
			int length = bytes.length;
			m_buf.writeInt(length);
			m_buf.writeBytes(bytes);
		}
	}

	public void writeBytes(ByteBuf buf) {
		if (null == buf) {
			writeNull();
		} else {
			m_buf.writeByte(Prefix.BYTES);
			int length = buf.readableBytes();
			m_buf.writeInt(length);
			m_buf.writeBytes(buf);
		}
	}

	public byte[] readBytes() {
		byte type = m_buf.readByte();

		if (Prefix.NULL != type) {
			return readBytes0();
		}
		return null;
	}

	private byte[] readBytes0() {
		byte[] bytes;
		int length = m_buf.readInt();
		bytes = new byte[length];
		m_buf.readBytes(bytes);
		return bytes;
	}

	public void writeString(String str) {
		if (null == str) {
			writeNull();
		} else {
			m_buf.writeByte(Prefix.STRING);
			int length = str.getBytes().length;
			m_buf.writeInt(length);
			m_buf.writeBytes(str.getBytes(Charsets.UTF_8));
		}
	}

	public String readString() {
		byte type = m_buf.readByte();
		if (Prefix.NULL != type) {
			return readString0();
		}
		return null;
	}

	private String readString0() {
		String result;
		int strLen = m_buf.readInt();
		byte[] strBytes = new byte[strLen];
		m_buf.readBytes(strBytes);
		result = new String(strBytes, Charsets.UTF_8);
		return result;
	}

	public void writeInt(int i) {
		m_buf.writeByte(Prefix.INT);
		m_buf.writeInt(i);
	}

	public int readInt() {
		m_buf.readByte(); // jump over Prefix.INT
		return m_buf.readInt();
	}

	public void writeChar(char c) {
		m_buf.writeByte(Prefix.CHAR);
		m_buf.writeChar(c);
	}

	public char readChar() {
		m_buf.readByte(); // jump over Prefix.CHAR
		return m_buf.readChar();
	}

	public void writeLong(long v) {
		m_buf.writeByte(Prefix.LONG);
		m_buf.writeLong(v);
	}

	public long readLong() {
		m_buf.readByte(); // jump over Prefix.LONG
		return m_buf.readLong();
	}

	public void writeList(List list) {
		if (null == list) {
			writeNull();
		} else {
			m_buf.writeByte(Prefix.LIST);
			m_buf.writeInt(list.size());

			// only write [[element type]...] if size > 0
			if (list.size() > 0) {
				for (Object o : list) {
					writeObject(o);
				}
			}
		}
	}

	/**
	 * @return new ArrayList()
	 */
	public List readList() {
		byte type = m_buf.readByte();
		if (Prefix.NULL != type) {
			return readList0();
		} else {
			return null;
		}
	}

	private List readList0() {
		int length = m_buf.readInt();
		// return ArrayList as default
		List result = new ArrayList<>();
		if (length > 0) {
			for (int i = 0; i < length; i++) {
				result.add(readObject());
			}
		}
		return result;
	}

	public void writeMap(Map<?, ?> map) {
		if (null == map) {
			writeNull();
		} else {
			m_buf.writeByte(Prefix.MAP);
			m_buf.writeInt(map.size());

			// only write [[key element][value element]...] if size > 0
			if (map.size() > 0) {
				for (Map.Entry<?, ?> entry : map.entrySet()) {
					// write [key element][value element][...][...]
					Object key = entry.getKey();
					Object value = entry.getValue();
					writeObject(key);
					writeObject(value);
				}
			}
		}
	}

	/**
	 * @return new HashMap()
	 */
	public Map readMap() {
		byte type = m_buf.readByte();
		if (Prefix.NULL == type) {
			return null;
		} else {
			return readMap0();
		}
	}

	private Map readMap0() {
		int length = m_buf.readInt();
		// return HashMap as default
		Map result = new HashMap();
		if (length > 0) {
			for (int i = 0; i < length; i++) {
				result.put(readObject(), readObject());
			}
		}
		return result;
	}

	public void writeNull() {
		m_buf.writeByte(Prefix.NULL);
	}

	public boolean isNextNull() {
		m_buf.markReaderIndex();
		byte type = m_buf.readByte();
		if (Prefix.NULL == type) {
			return true;
		} else {
			m_buf.resetReaderIndex();
			return false;
		}
	}

	public ByteBuf getBuf() {
		return m_buf;
	}

	public void writeObject(Object clazz) {
		if (null == clazz) {
			writeNull();
		} else if (clazz instanceof Boolean) {
			writeBoolean((Boolean) clazz);
		} else if (clazz instanceof byte[]) {
			writeBytes((byte[]) clazz);
		} else if (clazz instanceof Character) {
			writeChar((char) clazz);
		} else if (clazz instanceof Integer) {
			writeInt((int) clazz);
		} else if (clazz instanceof Long) {
			writeLong((long) clazz);
		} else if (clazz instanceof String) {
			writeString((String) clazz);
		} else if (clazz instanceof List) {
			writeList((List) clazz);
		} else if (clazz instanceof Map) {
			writeMap((Map) clazz);
		} else {
			throw new RuntimeException("Unsupported class type: " + clazz.getClass());
		}
	}

	private Object readObject() {
		byte type = m_buf.readByte();
		if (type == Prefix.NULL) {
			return null;
		} else if (type == Prefix.TRUE || type == Prefix.FALSE) {
			return readBoolean0(type);
		} else if (type == Prefix.BYTES) {
			return readBytes0();
		} else if (type == Prefix.CHAR) {
			return m_buf.readChar();
		} else if (type == Prefix.INT) {
			return m_buf.readInt();
		} else if (type == Prefix.LONG) {
			return m_buf.readLong();
		} else if (type == Prefix.STRING) {
			return readString0();
		} else if (type == Prefix.LIST) {
			return readList0();
		} else if (type == Prefix.MAP) {
			return readMap0();
		} else {
			throw new RuntimeException("Unsupported class type: " + type);
		}
	}

	public static int calLength(Object object) {
		if (null == object) {
			return NULL_LENGTH;
		} else if (object instanceof Boolean) {
			return BOOLEAN_LENGTH;
		} else if (object instanceof byte[]) {
			return calBytesLength((byte[]) object);
		} else if (object instanceof Character) {
			return CHAR_LENGTH;
		} else if (object instanceof Integer) {
			return INT_LENGTH;
		} else if (object instanceof Long) {
			return LONG_LENGTH;
		} else if (object instanceof String) {
			return calStringLength((String) object);
		} else if (object instanceof List) {
			return calListLength((List) object);
		} else if (object instanceof Map) {
			return calMapLength((Map) object);
		} else {
			throw new RuntimeException("Unsupported Class in Codec: " + object.getClass());
		}
	}

	public static final int NULL_LENGTH = 1;

	public static final int BOOLEAN_LENGTH = 1;

	public static final int CHAR_LENGTH = 3;

	public static final int INT_LENGTH = 5;

	public static final int LONG_LENGTH = 9;

	private static int calBytesLength(byte[] bytes) {
		return 1 + 4 + bytes.length;
	}

	private static int calListLength(List list) {
		int size = 0;
		size += 1; // Prefix.List
		size += 4; // int: list.size()

		for (Object o : list) {
			size += calLength(o);
		}
		return size;
	}

	private static int calStringLength(String s) {
		int size = 0;
		size += 1; // Prefix.String
		size += 4; // int; s.getBytes().length
		size += s.getBytes(Charsets.UTF_8).length;
		return size;
	}

	public static int calMapLength(Map map) {
		int size = 0;
		size += 1; // Prefix.Map
		size += 4; // Int: map.size()
		size += 2; // key element and value element.

		for (Object k : map.keySet()) {
			int keyLength = calLength(k);
			int valueLength = calLength(map.get(k));
			size += keyLength + valueLength;
		}
		return size;
	}

	private static class Prefix {

		static final byte NULL = Byte.parseByte("01", 16); // 0x01

		static final byte TRUE = Byte.parseByte("11", 16); // 0x11

		static final byte FALSE = Byte.parseByte("10", 16); // 0x10

		static final byte CHAR = Byte.parseByte("20", 16); // "0x20"

		static final byte BYTES = Byte.parseByte("24", 16); // "0x24"

		static final byte INT = Byte.parseByte("30", 16); // "0x30"

		static final byte LONG = Byte.parseByte("40", 16); // "0x40"

		static final byte STRING = Byte.parseByte("54", 16); // "0x54"

		static final byte LIST = Byte.parseByte("64", 16); // "0x64"

		static final byte MAP = Byte.parseByte("74", 16); // "0x74"
	}
}
