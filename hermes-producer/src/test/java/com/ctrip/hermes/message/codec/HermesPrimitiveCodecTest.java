package com.ctrip.hermes.message.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class HermesPrimitiveCodecTest extends ComponentTestCase {

	final static int dataSize = 10000;

	@Test public void testInt() {
		ByteBuffer bf = ByteBuffer.allocateDirect(10);

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeInt(5);
		codec.writeInt(1088);
		codec.bufFlip();
		assertEquals(5, codec.readInt());
		assertEquals(1088, codec.readInt());
	}

	@Test
	public void testBytes() {
		ByteBuffer bf = ByteBuffer.allocateDirect(100);

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);

		byte[] bytes = new byte[]{'a','c','f','u','n'};
		codec.writeBytes(bytes);
		codec.bufFlip();
		byte[] result = codec.readBytes();
		for (int i = 0; i < result.length; i ++) {
			assertEquals(bytes[i], result[i]);
		}
	}

	@Test
	public void testChar() {
		ByteBuffer bf = ByteBuffer.allocateDirect(100);

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeChar('a');
		codec.writeChar('c');
		codec.writeChar('f');
		codec.writeChar('u');
		codec.writeChar('n');
		codec.bufFlip();
		assertEquals('a', codec.readChar());
		assertEquals('c', codec.readChar());
		assertEquals('f', codec.readChar());
		assertEquals('u', codec.readChar());
		assertEquals('n', codec.readChar());
	}

	@Test public void testLong() {
		ByteBuffer bf = ByteBuffer.allocateDirect(27);

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeLong(5);
		codec.writeLong(1088);
		codec.writeLong(23918237891724L);
		codec.bufFlip();
		assertEquals(5, codec.readLong());
		assertEquals(1088, codec.readLong());
		assertEquals(23918237891724L, codec.readLong());
	}

	@Test public void testBoolean() {
		ByteBuffer bf = ByteBuffer.allocateDirect(3);

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeBoolean(true);
		codec.writeBoolean(false);
		codec.writeBoolean(false);
		codec.bufFlip();
		assertEquals(true, codec.readBoolean());
		assertEquals(false, codec.readBoolean());
		assertEquals(false, codec.readBoolean());
	}

	@Test public void testNull() {
		ByteBuffer bf = ByteBuffer.allocateDirect(3);

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(null);
		codec.writeMap(null);
		codec.bufFlip();
		assertNull(codec.readList());
		assertNull(codec.readMap());
	}

	@Test public void testString() {
		String a = null;
		String b = "longer string";
		String c = "中文字very very long string \n \r \n \b." + "very very long string \n" + " \n" + " \n" + " \b.";

		ByteBuffer bf = ByteBuffer.allocateDirect(5 + 0 +
				5 + b.getBytes().length + 5 + c.getBytes().length);

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeString(a);
		codec.writeString(b);
		codec.writeString(c);
		codec.bufFlip();
		String nullA = codec.readString();
		assertEquals(a, nullA);
		assertEquals(b, codec.readString());
		assertEquals(c, codec.readString());
	}

	@Test
	public void testArrayByInt() {
		// Integer
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < dataSize; i++) {
			list.add(i);
		}
		ByteBuffer bf = ByteBuffer.allocateDirect(10 * dataSize);
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		codec.bufFlip();
		List result = codec.readList();
		assertEquals(list, result);
	}

	@Test
	public void testArrayByLong() {
		//Long
		List<Long> list = new ArrayList<>();
		for (int i = 0; i < dataSize; i++) {
			list.add((long)i);
		}
		ByteBuffer bf = ByteBuffer.allocateDirect(10 * dataSize);
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		codec.bufFlip();
		List result = codec.readList();
		assertEquals(list, result);
	}

	@Test
	public void testArrayByString() {
		//String
		List<String> list = new ArrayList<>();
		for (int i = 0; i < dataSize; i++) {
			list.add(String.valueOf(i*i));
		}
		ByteBuffer bf = ByteBuffer.allocateDirect(100 * dataSize);
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		codec.bufFlip();
		List result = codec.readList();
		assertEquals(list, result);
	}

	@Test
	public void testArrayByList() {
		//List
		List<List<String>> list = new ArrayList<>();
		for (int i = 0; i < dataSize; i++) {
			List<String> tempList = new ArrayList<>();
			for (int j = 0; j < 10; j ++) {
				tempList.add(String.valueOf(j * i));
			}
			list.add(tempList);
		}

		ByteBuffer bf = ByteBuffer.allocateDirect(100 * dataSize);
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		codec.bufFlip();
		List result = codec.readList();
		assertEquals(list, result);
	}

	@Test
	public void testArrayByMap() {
		//List<Map>
		List<Map> list = new ArrayList<>();
		for (int i = 0; i < dataSize; i++) {
			Map<String, Long> tempMap = new HashMap<>();
			for (int j = 0; j < 10; j ++) {
				tempMap.put(String.valueOf(i * j), (long)i * j);
			}
			list.add(tempMap);
		}
		ByteBuffer bf = ByteBuffer.allocateDirect(200 * dataSize);
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		codec.bufFlip();
		List result = codec.readList();
		assertEquals(list, result);
	}

	@Test
	public void testMapByIntLong() {
		//Map<Integer, Long>
		Map<Integer, Long> map = new HashMap<>();
		for (int i = 0; i < dataSize; i++) {
			map.put(i, (long)i * i);
		}
		ByteBuffer bf = ByteBuffer.allocateDirect(200 * dataSize);
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeMap(map);
		codec.bufFlip();
		Map result = codec.readMap();
		assertEquals(map, result);
	}

	@Test
	public void testMapByStringList() {
		//Map<String, List>
		Map<String, List<String>> map = new HashMap<>();
		for (int i = 0; i < dataSize; i++) {
			List<String> tempList = new ArrayList<>();
			for(int j = 0; j < 10; j ++) {
				tempList.add(String.valueOf(i * j));
			}
			map.put(String.valueOf(i), tempList);
		}
		ByteBuffer bf = ByteBuffer.allocateDirect(200 * dataSize);
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeMap(map);
		codec.bufFlip();
		Map result = codec.readMap();
		assertEquals(map, result);
	}

	@Test
	public void testMapByListMap() {
		//Map<List, Map>
		Map<List<Long>, Map<String, Integer>> map = new HashMap<>();
		for (int i = 0; i < dataSize; i++) {
			List<Long> tempList = new ArrayList<>();
			for(int j = 0; j < 10; j++) {
				tempList.add((long) j);
			}
			Map<String, Integer> tempMap = new HashMap<>();
			for (int k = 0; k < 10; k ++) {
				tempMap.put(String.valueOf(k), k);
			}
			map.put(tempList, tempMap);
		}
		ByteBuffer bf = ByteBuffer.allocateDirect(100 * dataSize);
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeMap(map);
		codec.bufFlip();
		Map result = codec.readMap();
		assertEquals(map, result);
	}

	@Test
	public void testEmptyList() {
		List<Long> list = new ArrayList<>();

		ByteBuffer bf = ByteBuffer.allocateDirect(1000);
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		codec.bufFlip();
		List result = codec.readList();
		assertNotNull(result);
		assertEquals(list, result);
	}

	@Test
	public void testEmptyMap() {
		//Map<Integer, Long>
		Map<Integer, Long> map = new HashMap<>();
		ByteBuffer bf = ByteBuffer.allocateDirect(1000);
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeMap(map);
		codec.bufFlip();
		Map result = codec.readMap();
		assertNotNull(result);
		assertEquals(map, result);
	}
}