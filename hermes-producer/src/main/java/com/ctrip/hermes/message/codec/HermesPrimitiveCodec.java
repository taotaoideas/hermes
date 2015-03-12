package com.ctrip.hermes.message.codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;

/**
 * Element      Prefix
 * +--------------------------------------------------------------+
 * Fixed Width:                          Following Octets
 * null         0x01                     0 OCTET
 * boolean      0x10(false) 0x11(true)   0 OCTET
 * char         0x20                     2 OCTET
 * int          0x30                     4 OCTET
 * long         0x40                     8 OCTET
 * +--------------------------------------------------------------+
 * Variable Width:                       Following Data Size
 * Bytes        0x24                     4 OCTET  (max size:2^32 = 2G)
 * String       0x54                     4 OCTET  (max size:2^32 = 2G)
 * +----------------------------------------------+---------------+
 * Array:       [count][Element][...]
 * 0x64                     followed 4 octet of data count
 * Map:         [count][[key Element][value Element]][...]
 * 0x74                     followed 4 octet of data count
 */
public class HermesPrimitiveCodec {

	private ByteBuffer m_buf;

	public HermesPrimitiveCodec(ByteBuffer buf) {
		m_buf = buf;
		m_buf.order(ByteOrder.BIG_ENDIAN);
	}

	public void bufFlip() {
		m_buf.flip();
	}

	public void writeBoolean(boolean b) {
		m_buf.put(b ? Prefix.TRUE : Prefix.FALSE);
	}

	public void writeBytes(byte[] bytes) {
		if (null == bytes) {
			writeNull();
		} else {
			m_buf.put(Prefix.BYTES);
			int length = bytes.length;
			m_buf.putInt(length);
			m_buf.put(bytes);
		}
	}

	public byte[] readBytes() {
		byte[] bytes = null;
		byte type = m_buf.get();

		if (Prefix.NULL != type) {
			int length = m_buf.getInt();
			bytes = new byte[length];
			m_buf.get(bytes);
		}
		return bytes;
	}

	public void writeString(String str) {
		if (null == str) {
			writeNull();
		} else {
			m_buf.put(Prefix.STRING);
			int length = str.getBytes().length;
			m_buf.putInt(length);
			m_buf.put(str.getBytes(Charsets.UTF_8));
		}
	}

	public String readString() {
		String result = null;
		byte type = m_buf.get();

		if (Prefix.NULL != type) {
			int strLen = m_buf.getInt();
			byte[] strBytes = new byte[strLen];
			m_buf.get(strBytes);
			result = new String(strBytes, Charsets.UTF_8);
		}
		return result;
	}

	public void writeInt(int i) {
		m_buf.put(Prefix.INT);
		m_buf.putInt(i);
	}

	public boolean readBoolean() {
		byte b = m_buf.get();
		return (b & 0x1) == 1;
	}

	public int readInt() {
		m_buf.get();  // jump over Prefix.INT
		return m_buf.getInt();
	}

	public void writeChar(char c) {
		m_buf.put(Prefix.CHAR);
		m_buf.putChar(c);
	}

	public char readChar() {
		m_buf.get(); // jump over Prefix.CHAR
		return m_buf.getChar();
	}

	public void writeLong(long v) {
		m_buf.put(Prefix.LONG);
		m_buf.putLong(v);
	}

	public long readLong() {
		m_buf.get(); // jump over Prefix.LONG
		return m_buf.getLong();
	}

	public void writeList(List list) {
		if (null == list) {
			writeNull();
		} else {
			m_buf.put(Prefix.LIST);
			m_buf.putInt(list.size());

			// only write [[element type]...] if size > 0
			if (list.size() > 0) {
				Object clazz = list.get(0);
				m_buf.put(ClassToByte(clazz));
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
		byte type = m_buf.get();
		if (Prefix.NULL == type) {
			return null;
		} else {
			int length = m_buf.getInt();
			// return ArrayList as default
			List result = new ArrayList<>();
			if (length > 0) {
				byte listType = m_buf.get();

				Object clazz = byteToClass(listType);

				for (int i = 0; i < length; i++) {
					result.add(readObject(clazz));
				}
			}
			return result;
		}
	}

	public void writeMap(Map map) {
		if (null == map) {
			writeNull();
		} else {
			m_buf.put(Prefix.MAP);
			m_buf.putInt(map.size());

			// only write [[key element][value element]...] if size > 0
			if (map.size() > 0) {
				Object key = null, value = null;
				for (Object keys : map.keySet()) {
					key = keys;
					break;
				}
				for (Object values : map.values()) {
					value = values;
					break;
				}

				// write [key element][value element]
				m_buf.put(ClassToByte(key));
				m_buf.put(ClassToByte(value));

				// todo: change to for(Map.entrySet()) loop
				for (Object tempKey : map.keySet()) {
					Object tempValue = map.get(tempKey);
					writeObject(tempKey);
					writeObject(tempValue);
				}
			}
		}
	}

	/**
	 * @return new HashMap()
	 */
	public Map readMap() {
		byte type = m_buf.get();
		if (Prefix.NULL == type) {
			return null;
		} else {
			int length = m_buf.getInt();
			// return HashMap as default
			Map result = new HashMap();
			if (length > 0) {
				byte keyType = m_buf.get();
				byte valueType = m_buf.get();
				Object keyClazz = byteToClass(keyType);
				Object valueClazz = byteToClass(valueType);

				for (int i = 0; i < length; i++) {
					result.put(readObject(keyClazz), readObject(valueClazz));
				}
			}
			return result;
		}
	}

	public void writeNull() {
		m_buf.put(Prefix.NULL);
	}

	public boolean isNextNull() {
		m_buf.mark();
		byte type = m_buf.get();
		if (Prefix.NULL == type) {
			return true;
		} else {
			m_buf.reset();
			return false;
		}
	}

	public ByteBuffer getBuf() {
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

	private Object readObject(Object clazz) {
		if (null == clazz) {
			return null;
		} else if (clazz instanceof Boolean) {
			return readBoolean();
		} else if (clazz instanceof byte[]) {
			return readChar();
		} else if (clazz.equals(Character.class)) {
			return readChar();
		} else if (clazz.equals(Integer.class)) {
			return readInt();
		} else if (clazz.equals(Long.class)) {
			return readLong();
		} else if (clazz.equals(String.class)) {
			return readString();
		} else if (clazz.equals(List.class)) {
			return readList();
		} else if (clazz.equals(Map.class)) {
			return readMap();
		} else {
			throw new RuntimeException("Unsupported class type: " + clazz.getClass());
		}
	}

	/**
	 * type: won't be Prefix.NULL
	 */
	private Object byteToClass(byte type) {
		if (type == Prefix.TRUE) {
			return Boolean.TRUE;
		} else if (type == Prefix.FALSE) {
			return Boolean.FALSE;
		} else if (type == Prefix.BYTES) {
			return Byte[].class;
		} else if (type == Prefix.CHAR) {
			return Character.class;
		} else if (type == Prefix.INT) {
			return Integer.class;
		} else if (type == Prefix.LONG) {
			return Long.class;
		} else if (type == Prefix.STRING) {
			return String.class;
		} else if (type == Prefix.LIST) {
			return List.class;
		} else if (type == Prefix.MAP) {
			return Map.class;
		} else {
			throw new RuntimeException("Unknown type in Codec: " + type);
		}
	}

	private byte ClassToByte(Object object) {
		if (null == object) {
			return Prefix.NULL;
		} else if (object instanceof Boolean) {
			return ((boolean) object) ? Prefix.TRUE : Prefix.FALSE;
		} else if (object instanceof Character) {
			return Prefix.CHAR;
		} else if (object instanceof byte[]) {
			return Prefix.BYTES;
		} else if (object instanceof Integer) {
			return Prefix.INT;
		} else if (object instanceof Long) {
			return Prefix.LONG;
		} else if (object instanceof String) {
			return Prefix.STRING;
		} else if (object instanceof List) {
			return Prefix.LIST;
		} else if (object instanceof Map) {
			return Prefix.MAP;
		} else {
			throw new RuntimeException("Unsupported Class in Codec: " + object.getClass());
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

	public static final int CHAR_LENGTH = 2;

	public static final int INT_LENGTH = 4;

	public static final int LONG_LENGTH = 9;

	public static final int STRING_LENGTH = -1;

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
		size += s.getBytes().length;
		return size;
	}

	public static int calMapLength(Map map) {
		int size = 0;
		size += 1;  // Prefix.Map
		size += 4;  // Int: map.size()

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

		static final byte FALSE = Byte.parseByte("10", 16); //0x10

		static final byte CHAR = Byte.parseByte("20", 16); //"0x20"

		static final byte BYTES = Byte.parseByte("24", 16); //"0x24"

		static final byte INT = Byte.parseByte("30", 16); //"0x30"

		static final byte LONG = Byte.parseByte("40", 16); //"0x40"

		static final byte STRING = Byte.parseByte("54", 16); //"0x54"

		static final byte LIST = Byte.parseByte("64", 16); //"0x64"

		static final byte MAP = Byte.parseByte("74", 16); //"0x74"
	}
}
