package com.ctrip.hermes.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.*;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.google.common.base.Charsets;

@SuppressWarnings({ "rawtypes" })
public class HermesPrimitiveCodecTest extends ComponentTestCase {

	final static int dataSize = 10000;

	@Test
	public void testInt() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeInt(5);
		codec.writeInt(1088);
		assertEquals(5, codec.readInt());
		assertEquals(1088, codec.readInt());
	}

	@Test
	public void testBytes() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);

		byte[] bytes = new byte[] { 'a', 'c', 'f', 'u', 'n' };
		codec.writeBytes(bytes);
		byte[] result = codec.readBytes();
		for (int i = 0; i < result.length; i++) {
			assertEquals(bytes[i], result[i]);
		}
	}

	@Test
	public void testChar() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeChar('a');
		codec.writeChar('c');
		codec.writeChar('f');
		codec.writeChar('u');
		codec.writeChar('n');
		assertEquals('a', codec.readChar());
		assertEquals('c', codec.readChar());
		assertEquals('f', codec.readChar());
		assertEquals('u', codec.readChar());
		assertEquals('n', codec.readChar());
	}

	@Test
	public void testLong() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeLong(5);
		codec.writeLong(1088);
		codec.writeLong(23918237891724L);
		assertEquals(5, codec.readLong());
		assertEquals(1088, codec.readLong());
		assertEquals(23918237891724L, codec.readLong());
	}

	@Test
	public void testBoolean() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeBoolean(true);
		codec.writeBoolean(false);
		codec.writeBoolean(false);
		assertEquals(true, codec.readBoolean());
		assertEquals(false, codec.readBoolean());
		assertEquals(false, codec.readBoolean());
	}

	@Test
	public void testNull() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(null);
		codec.writeMap(null);
		assertNull(codec.readList());
		assertNull(codec.readMap());
	}

	@Test
	public void testString() {
		String a = null;
		String b = "longer string";
		String c = "中文字very very long string \n \r \n \b." + "very very long string \n" + " \n" + " \n" + " \b.";
		String strangeString = "{\"1\":{\"str\":\"429bb071\"}," +
				  "\"2\":{\"s\":\"ExchangeTest\"},\"3\":{\"i32\":8},\"4\":{\"str\":\"uft-8\"}," +
				  "\"5\":{\"str\":\"cmessage-adapter 1.0\"},\"6\":{\"i32\":3},\"7\":{\"i32\":1}," +
				  "\"8\":{\"i32\":0},\"9\":{\"str\":\"order_new\"},\"10\":{\"str\":\"\"}," +
				  "\"11\":{\"str\":\"1\"},\"12\":{\"str\":\"DST56615\"},\"13\":{\"str\":\"555555\"}," +
				  "\"14\":{\"str\":\"169.254.142.159\"},\"15\":{\"str\":\"java.lang.String\"}," +
				  "\"16\":{\"i64\":1429168996889},\"17\":{\"map\":[\"str\",\"str\",0,{}]}}";

		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeString(a);
		codec.writeString(b);
		codec.writeString(c);
		codec.writeString(strangeString);
		String nullA = codec.readString();
		assertEquals(a, nullA);
		assertEquals(b, codec.readString());
		assertEquals(c, codec.readString());
		assertEquals(strangeString, codec.readString());
	}

	@Test
	public void testArrayByInt() {
		// Integer
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < dataSize; i++) {
			list.add(i);
		}
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		List result = codec.readList();
		assertEquals(list, result);
	}

	@Test
	public void testArrayByLong() {
		// Long
		List<Long> list = new ArrayList<>();
		for (int i = 0; i < dataSize; i++) {
			list.add((long) i);
		}
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		List result = codec.readList();
		assertEquals(list, result);
	}

	@Test
	public void testArrayByString() {
		// String
		List<String> list = new ArrayList<>();
		for (int i = 0; i < dataSize; i++) {
			list.add(String.valueOf(i * i));
		}
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		List result = codec.readList();
		assertEquals(list, result);
	}

	@Test
	public void testArrayByList() {
		// List
		List<List<String>> list = new ArrayList<>();
		for (int i = 0; i < dataSize; i++) {
			List<String> tempList = new ArrayList<>();
			for (int j = 0; j < 10; j++) {
				tempList.add(String.valueOf(j * i));
			}
			list.add(tempList);
		}

		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		List result = codec.readList();
		assertEquals(list, result);
	}

	@Test
	public void testArrayByMap() {
		// List<Map>
		List<Map> list = new ArrayList<>();
		for (int i = 0; i < dataSize; i++) {
			Map<String, Long> tempMap = new HashMap<>();
			for (int j = 0; j < 10; j++) {
				tempMap.put(String.valueOf(i * j), (long) i * j);
			}
			list.add(tempMap);
		}
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		List result = codec.readList();
		assertEquals(list, result);
	}

	@Test
	public void testMapByIntLong() {
		// Map<Integer, Long>
		Map<Integer, Long> map = new HashMap<>();
		for (int i = 0; i < dataSize; i++) {
			map.put(i, (long) i * i);
		}
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeMap(map);
		Map result = codec.readMap();
		assertEquals(map, result);
	}

	@Test
	public void testMapByStringList() {
		// Map<String, List>
		Map<String, List<String>> map = new HashMap<>();
		for (int i = 0; i < dataSize; i++) {
			List<String> tempList = new ArrayList<>();
			for (int j = 0; j < 10; j++) {
				tempList.add(String.valueOf(i * j));
			}
			map.put(String.valueOf(i), tempList);
		}
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeMap(map);
		Map result = codec.readMap();
		assertEquals(map, result);
	}

	@Test
	public void testMapByListMap() {
		// Map<List, Map>
		Map<List<Long>, Map<String, Integer>> map = new HashMap<>();
		for (int i = 0; i < dataSize; i++) {
			List<Long> tempList = new ArrayList<>();
			for (int j = 0; j < 10; j++) {
				tempList.add((long) j);
			}
			Map<String, Integer> tempMap = new HashMap<>();
			for (int k = 0; k < 10; k++) {
				tempMap.put(String.valueOf(k), k);
			}
			map.put(tempList, tempMap);
		}
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeMap(map);
		Map result = codec.readMap();
		assertEquals(map, result);
	}

	@Test
	public void testEmptyList() {
		List<Long> list = new ArrayList<>();

		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeList(list);
		List result = codec.readList();
		assertNotNull(result);
		assertEquals(list, result);
	}

	@Test
	public void testEmptyMap() {
		// Map<Integer, Long>
		Map<Integer, Long> map = new HashMap<>();
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeMap(map);
		Map result = codec.readMap();
		assertNotNull(result);
		assertEquals(map, result);
	}

	@Test
	public void testMapByStringString() {

//		final String strangeString = "{\"1\":{\"str\":\"429bb071\"},\"2\":{\"str\":\"ExchangeTest\"},\"3\":{\"i32\":8},\"4\":{\"str\":\"uft-8\"},\"5\":{\"str\":\"cmessage-adapter 1.0\"},";

		final String strangeString = "{\"1\":{\"str\":\"429bb071\"}," +
				  "\"2\":{\"s\":\"ExchangeTest\"},\"3\":{\"i32\":8},\"4\":{\"str\":\"uft-8\"}," +
				  "\"5\":{\"str\":\"cmessage-adapter 1.0\"},\"6\":{\"i32\":3},\"7\":{\"i32\":1}," +
				  "\"8\":{\"i32\":0},\"9\":{\"str\":\"order_new\"},\"10\":{\"str\":\"\"}," +
				  "\"11\":{\"str\":\"1\"},\"12\":{\"str\":\"DST56615\"},\"13\":{\"str\":\"555555\"}," +
				  "\"14\":{\"str\":\"169.254.142.159\"},\"15\":{\"str\":\"java.lang.String\"}," +
				  "\"16\":{\"i64\":1429168996889},\"17\":{\"map\":[\"str\",\"str\",0,{}]}}";

		Map<String, String> map = new HashMap<>();
		map.put("string", strangeString);

		ByteBuf bfInit = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bfInit);
		codec.writeMap(map);

		ByteBuf buf1 =codec.getBuf();
		byte[] bytes = new byte[buf1.readableBytes()];
		buf1.readBytes(bytes);
		System.out.println(Arrays.toString(bytes));


		// 由于UTF_8是变长的编码，对于HermesPrimitiveCodec的一些自定值会错误的展开，导致最后deCode失败。
		// 改成ISO_8859_1可以通过该Test，因为ISO_8859_1是定长编码。
		byte[] outBytes = new String(bytes, Charsets.UTF_8).getBytes(Charsets.UTF_8);

		assertEquals(Arrays.toString(bytes), Arrays.toString(outBytes));

		System.out.println(Arrays.toString(outBytes));
		ByteBuf buf2 = (Unpooled.wrappedBuffer(outBytes));
		HermesPrimitiveCodec codec2 = new HermesPrimitiveCodec(buf2);


		Map result = codec2.readMap();
		assertEquals(map, result);
	}
}