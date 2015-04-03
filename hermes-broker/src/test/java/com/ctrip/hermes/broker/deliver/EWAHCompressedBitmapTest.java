package com.ctrip.hermes.broker.deliver;

import java.util.Random;

import org.junit.Test;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class EWAHCompressedBitmapTest {

	@Test
	public void testMonoIncreSet() {
		EWAHCompressedBitmap bm = new EWAHCompressedBitmap();

		long start = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			bm.set(i);
		}
		System.out.println("Mono incre set: " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testMonoDecreSet() {
		EWAHCompressedBitmap bm = new EWAHCompressedBitmap();

		long start = System.currentTimeMillis();
		for (int i = 1000000; i > 0; i--) {
			bm.set(i);
		}
		System.out.println("Mono decre set: " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testRandomSet() {
		EWAHCompressedBitmap bm = new EWAHCompressedBitmap();
		int[] ints = new int[1000000];

		Random rnd = new Random(System.currentTimeMillis());
		for (int i = 0; i < ints.length; i++) {
			ints[i] = rnd.nextInt(1000000);
		}

		long start = System.currentTimeMillis();
		for (int i = 0; i < ints.length; i++) {
			bm.set(i);
		}
		System.out.println("Random set: " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testMonoIncreClear() {
		EWAHCompressedBitmap bm = new EWAHCompressedBitmap();

		for (int i = 0; i < 1000000; i++) {
			bm.set(i);
		}

		long start = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			bm.clear(i);
			bm.isEmpty();
		}
		System.out.println("Mono incre clear and isEmpty(): " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testMonoDecreClear() {
		EWAHCompressedBitmap bm = new EWAHCompressedBitmap();

		for (int i = 0; i < 1000000; i++) {
			bm.set(i);
		}

		long start = System.currentTimeMillis();
		for (int i = 1000000; i > 0; i--) {
			bm.clear(i);
			bm.isEmpty();
		}
		System.out.println("Mono decre clear and isEmpty(): " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testRandomClean() {
		EWAHCompressedBitmap bm = new EWAHCompressedBitmap();
		int[] ints = new int[1000000];

		Random rnd = new Random(System.currentTimeMillis());
		for (int i = 0; i < ints.length; i++) {
			ints[i] = rnd.nextInt(1000000);
		}

		for (int i = 0; i < 1000000; i++) {
			bm.set(i);
		}

		long start = System.currentTimeMillis();
		for (int i = 0; i < ints.length; i++) {
			bm.clear(i);
			bm.isEmpty();
		}
		System.out.println("Random clear and isEmpty(): " + (System.currentTimeMillis() - start));
	}

}
