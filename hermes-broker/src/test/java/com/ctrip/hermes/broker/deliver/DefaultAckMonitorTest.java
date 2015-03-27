package com.ctrip.hermes.broker.deliver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.storage.ContinuousRange;
import com.ctrip.hermes.storage.storage.Locatable;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;

public class DefaultAckMonitorTest {

	private DefaultAckMonitor<String> m;

	@Before
	public void before() {
		int factor = 1000;
		m = new DefaultAckMonitor<String>(5000 * factor, 1000 * factor);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSingleSuccess() throws Exception {
		String expId = uuid();
		CountDownLatch latch = new CountDownLatch(1);
		AtomicBoolean shouldTimeout = new AtomicBoolean(false);
		check(m, latch, shouldTimeout, expId, 1, Arrays.asList(0), Collections.EMPTY_LIST, new int[] { 0, 0 }, new int[0]);
		assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSingleFail() throws Exception {
		String expId = uuid();
		CountDownLatch latch = new CountDownLatch(2);
		AtomicBoolean shouldTimeout = new AtomicBoolean(false);
		check(m, latch, shouldTimeout, expId, 1, Collections.EMPTY_LIST, Arrays.asList(0), new int[] { 0, 0 }, new int[] {
		      0, 0 });
		assertTrue(latch.await(200, TimeUnit.DAYS));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSingleTimeout() throws Exception {
		final AtomicBoolean shouldTimeout = new AtomicBoolean(false);
		m = new DefaultAckMonitor<String>(10, 10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return shouldTimeout.get();
			}

		};
		String expId = uuid();
		CountDownLatch latch = new CountDownLatch(2);
		check(m, latch, shouldTimeout, expId, 1, Collections.EMPTY_LIST, Collections.EMPTY_LIST, new int[] { 0, 0 },
		      new int[] { 0, 0 });
		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMixed1() throws Exception {
		final AtomicBoolean shouldTimeout = new AtomicBoolean(false);
		m = new DefaultAckMonitor<String>(10, 10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return shouldTimeout.get();
			}

		};
		String expId = uuid();
		CountDownLatch latch = new CountDownLatch(2);
		check(m, latch, shouldTimeout, expId, 10, Arrays.asList(0, 1, 2, 9), Collections.EMPTY_LIST, //
		      new int[] { 0, 9 }, new int[] { 3, 8 });
		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMixed2() throws Exception {
		final AtomicBoolean shouldTimeout = new AtomicBoolean(false);
		m = new DefaultAckMonitor<String>(10, 10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return shouldTimeout.get();
			}

		};
		String expId = uuid();
		CountDownLatch latch = new CountDownLatch(2);
		check(m, latch, shouldTimeout, expId, 10, Arrays.asList(1, 5, 8), Collections.EMPTY_LIST, //
		      new int[] { 0, 9 }, new int[] { 0, 9 });
		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testMixed3() throws Exception {
		final AtomicBoolean shouldTimeout = new AtomicBoolean(false);
		m = new DefaultAckMonitor<String>(10, 10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return shouldTimeout.get();
			}

		};
		String expId = uuid();
		CountDownLatch latch = new CountDownLatch(2);
		check(m, latch, shouldTimeout, expId, 10, Arrays.asList(1, 5, 8), Arrays.asList(2, 6, 9), //
		      new int[] { 0, 9 }, new int[] { 0, 9 });
		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
	}

	private void check(DefaultAckMonitor<String> m, final CountDownLatch latch, //
	      AtomicBoolean timeout, String expId, int totalOffsets, //
	      List<Integer> successes, List<Integer> fails, //
	      final int[] successIdxes, final int[] failIdxes) throws Exception {
		// offsets
		List<Integer> offsets = new ArrayList<>();
		Random rnd = new Random(System.currentTimeMillis());
		for (int i = 0; i < totalOffsets; i++) {
			int offset = rnd.nextInt(10000);
			offsets.add(offset);
		}
		Collections.sort(offsets);
		System.out.println("Deliver " + offsets);

		// locatables
		final List<Locatable> locatables = new ArrayList<>();
		final String expCtx = uuid();
		for (Integer offset : offsets) {
			locatables.add(new MyLocatable(expId, offset));
		}

		final AtomicReference<Boolean> onSuccessCalled = new AtomicReference<Boolean>(false);
		m.addListener(new AckStatusListener<String>() {

			@Override
			public void onSuccess(Range range, String ctx) {
				System.out.println("onSuccess " + range);
				onSuccessCalled.set(true);

				assertEquals(expCtx, ctx);
				assertEquals(
				      new ContinuousRange(locatables.get(successIdxes[0]).getOffset(), locatables.get(successIdxes[1])
				            .getOffset()), range);

				latch.countDown();
			}

			@Override
			public void onFail(Range range, String ctx) {
				System.out.println("onFail " + range);
				if (failIdxes.length <= 0) {
					fail();
				}

				// onFail should be called before onSuccess
				assertFalse(onSuccessCalled.get());

				onSuccessCalled.set(true);
				assertEquals(expCtx, ctx);
				assertEquals(new ContinuousRange(locatables.get(failIdxes[0]).getOffset(), locatables.get(failIdxes[1])
				      .getOffset()), range);

				latch.countDown();
			}

		});

		m.delivered(locatables, expCtx);

		// ack
		for (Integer success : successes) {
			m.acked(locatables.get(success), Ack.SUCCESS);
		}
		for (Integer fail : fails) {
			m.acked(locatables.get(fail), Ack.FAIL);
		}

		timeout.set(true);

	}

	private String uuid() {
		return UUID.randomUUID().toString();
	}

	static class MyLocatable implements Locatable {

		private Offset m_offset;

		public MyLocatable(String id, long offset) {
			m_offset = new Offset(id, offset);
		}

		@Override
		public void setOffset(Offset offset) {
			m_offset = offset;
		}

		@Override
		public Offset getOffset() {
			return m_offset;
		}

	}

}
