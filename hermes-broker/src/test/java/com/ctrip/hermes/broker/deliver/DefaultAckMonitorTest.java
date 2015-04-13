package com.ctrip.hermes.broker.deliver;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

public class DefaultAckMonitorTest {

	private DefaultAckMonitor<String> m;

	@Before
	public void before() {
		m = new DefaultAckMonitor<String>(5000);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSingleSuccess() throws Exception {
		String expId = uuid();
		check(m, expId, 1, Arrays.asList(0), Collections.EMPTY_LIST, new int[] { 0, 0 }, new int[0]);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSingleFail() throws Exception {
		String expId = uuid();
		check(m, expId, 1, Collections.EMPTY_LIST, Arrays.asList(0), new int[] { 0, 0 }, new int[] { 0 });
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSingleTimeout() throws Exception {
		m = new DefaultAckMonitor<String>(10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return true;
			}

		};
		String expId = uuid();
		check(m, expId, 1, Collections.EMPTY_LIST, Collections.EMPTY_LIST, new int[] { 0, 0 }, new int[] { 0 });
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMixed1() throws Exception {
		m = new DefaultAckMonitor<String>(10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return true;
			}

		};
		String expId = uuid();
		check(m, expId, 10, Arrays.asList(0, 1, 2, 9), Collections.EMPTY_LIST, //
		      new int[] { 0, 9 }, new int[] { 3, 4, 5, 6, 7, 8 });
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMixed2() throws Exception {
		m = new DefaultAckMonitor<String>(10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return true;
			}

		};
		String expId = uuid();
		check(m, expId, 10, Arrays.asList(1, 5, 8), Collections.EMPTY_LIST, //
		      new int[] { 0, 9 }, new int[] { 0, 2, 3, 4, 6, 7, 9 });
	}

	@Test
	public void testMixed3() throws Exception {
		m = new DefaultAckMonitor<String>(10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return true;
			}

		};
		String expId = uuid();
		check(m, expId, 10, Arrays.asList(1, 5, 8), Arrays.asList(2, 6, 9), //
		      new int[] { 0, 9 }, new int[] { 0, 2, 3, 4, 6, 7, 9 });
	}

	private void check(DefaultAckMonitor<String> m, //
	      final String expId, int totalOffsets, //
	      List<Integer> successes, List<Integer> fails, //
	      final int[] successIdxes, final int[] failIdxes) throws Exception {
		// offsets
		final List<Integer> offsets = new ArrayList<>();
		Random rnd = new Random(System.currentTimeMillis());
		for (int i = 0; i < totalOffsets; i++) {
			int offset = rnd.nextInt(10000);
			offsets.add(offset);
		}
		Collections.sort(offsets);

		// locatables
		final EnumRange<String> locatables = new EnumRange<>(expId);
		for (Integer offset : offsets) {
			locatables.addOffset(offset);

			// test merge
			EnumRange<String> subRange = new EnumRange<String>(expId);
			subRange.addOffset(offset);
			m.delivered(subRange);
		}

		// ack
		for (Integer success : successes) {
			m.acked(new Locatable<>(expId, locatables.getOffsets().get(success)), true);
		}
		for (Integer fail : fails) {
			m.acked(new Locatable<>(expId, locatables.getOffsets().get(fail)), false);
		}

		BatchResult batchResult = m.scan();
		ContinuousRange<?> doneRange = batchResult.getDoneRange();
		EnumRange<?> failRange = batchResult.getFailRange();

		System.out.println("Range done: " + doneRange);
		assertEquals(new ContinuousRange<>(expId, offsets.get(successIdxes[0]), offsets.get(successIdxes[1])), doneRange);

		if (failIdxes.length > 0) {
			System.out.println("Range fail: " + failRange);

			EnumRange<String> expRange = new EnumRange<>(expId);
			for (Integer failIdx : failIdxes) {
				expRange.addOffset(offsets.get(failIdx));
			}
			assertEquals(expRange, failRange);
		}

	}

	private String uuid() {
		return UUID.randomUUID().toString();
	}

}
