package com.ctrip.hermes.rangemonitor;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.range.Offset;
import com.ctrip.hermes.range.Range;
import com.ctrip.hermes.range.RangeMonitor;
import com.ctrip.hermes.range.RangeStatusListener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangeMonitorTest extends ComponentTestCase {
    private static final String id = "RangeMonitorTest";

    // 最大到百万级别
    private final int batchSize = 100 * 10000; // one million.
    private final int latchWaitSecond = 10;


    @Test
    public void allSuccess() throws InterruptedException {
        RangeMonitor monitor = lookup(RangeMonitor.class);
        final CountDownLatch latch = new CountDownLatch(1);


        monitor.addListener(new RangeStatusListener() {
            @Override
            public void onRangeDone(Range innerRange) {
                assertEquals(1L, innerRange.startOffset().getOffset());
                assertEquals(batchSize, innerRange.endOffset().getOffset());
                System.out.println("countDownLatch do countDown");
                latch.countDown();
            }

            @Override
            public void onRangeFail(Range range) {
                throw new RuntimeException("should not run into this.");
            }
        });


        startNewOffset(monitor, 1, batchSize);

        long start = new Date().getTime();

        for (int i = 1; i <= batchSize; i++) {
            monitor.offsetDone(new Offset(id, i), true);
        }
        System.out.println("all offset done: " + (new Date().getTime() - start) + "ms" );

        assertTrue(latch.await(latchWaitSecond, TimeUnit.SECONDS));
    }

    private void startNewOffset(RangeMonitor monitor, int start, int end) {
        for (int i = start; i <= end; i++)
            monitor.startNewOffsets(new Offset(id, i));
    }


    @Test
    public void allFail() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        RangeMonitor monitor = lookup(RangeMonitor.class);
        startNewOffset(monitor, 1, batchSize);
        monitor.addListener(new RangeStatusListener() {
            @Override
            public void onRangeDone(Range innerRange) {
                assertEquals(1L, innerRange.startOffset().getOffset());
                assertEquals(batchSize, innerRange.endOffset().getOffset());
                latch.countDown();
            }

            @Override
            public void onRangeFail(Range innerRange) {
                assertEquals(1L, innerRange.startOffset().getOffset());
                assertEquals(batchSize, innerRange.endOffset().getOffset());
                latch.countDown();
            }
        });

        for (int i = 1; i <= batchSize; i++) {
            monitor.offsetDone(new Offset(id, i), false);
        }

        assertTrue(latch.await(latchWaitSecond, TimeUnit.SECONDS));
    }


    @Test
    public void someAck() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(batchSize / 10 + 1);
        RangeMonitor monitor = lookup(RangeMonitor.class);
        startNewOffset(monitor, 1, batchSize);
        monitor.addListener(new RangeStatusListener() {
            @Override
            public void onRangeDone(Range innerRange) {
                assertEquals(1L, innerRange.startOffset().getOffset());
                assertEquals(batchSize, innerRange.endOffset().getOffset());
                latch.countDown();
            }

            @Override
            public void onRangeFail(Range inerRange) {
                // todo: 聚合起来所有Fail的再比较
                assertTrue(inerRange.startOffset().getOffset() % 10 == 0);
                latch.countDown();
            }
        });

        for (int i = 1; i <= batchSize; i++) {
            if (i % 10 == 0) {
                monitor.offsetDone(new Offset(id, i), false);
            } else {
                monitor.offsetDone(new Offset(id, i), true);
            }
        }
        assertTrue(latch.await(latchWaitSecond, TimeUnit.SECONDS));
    }

    @Test
    public void allTimeout() {
        //todo:


    }

    @Test
    public void multiThread() {

    }

    private void assertOffsetEqual(Offset offset1, Offset offset2) {
        assertTrue(offset1.getId().equals(offset2.getId())
                && offset1.getOffset() == offset2.getOffset());
    }

}
