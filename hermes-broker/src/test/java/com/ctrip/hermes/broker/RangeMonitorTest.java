package com.ctrip.hermes.broker;

import org.unidal.lookup.ComponentTestCase;

public class RangeMonitorTest extends ComponentTestCase {
  /*  private static final String id = "RangeMonitorTest";

    // 最大到百万级别
    private final int batchSize = 100 ;
    private final int latchWaitSecond = 10;


    @Test
    public void allSuccess() throws InterruptedException {
        RangeMonitor monitor = lookup(RangeMonitor.class);
        final CountDownLatch latch = new CountDownLatch(1);


        monitor.addListener(new RangeStatusListener() {
            @Override
            public void onRangeSuccess(Range innerRange) {
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
        System.out.println("all offset done: " + (new Date().getTime() - start) + "ms");

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
    public void multiThreadMostSuccess() throws InterruptedException {
        final RangeMonitor monitor = lookup(RangeMonitor.class);
        final int batchMaxLength = 100;


        final List<Integer> randomOrder = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            randomOrder.add(i);
        }
//        Collections.shuffle(randomOrder);

        final Set<Integer> doneSet = new HashSet<>();
        final Set<Integer> failSet = new HashSet<>();
        final Set<Integer> ackFailSet = new HashSet<>();


        monitor.addListener(new RangeStatusListener() {
            @Override
            public void onRangeDone(Range range) {
                for (long i = range.getStartOffset().getOffset(); i <= range.endOffset().getOffset(); i++)
                    doneSet.add((int) i);
            }

            @Override
            public void onRangeFail(Range range) {
                for (long i = range.getStartOffset().getOffset(); i <= range.endOffset().getOffset(); i++)
                    failSet.add((int) i);
            }
        });



        Runnable receiveThread = new Runnable() {
            @Override
            public void run() {
                //放入一批offset (随机的，离散的，不连续的)
                for (int i = 0; i < randomOrder.size(); ) {
                    int fromIndex = i, toIndex;
                    int oneBatch = new Random().nextInt(batchMaxLength);
                    if (fromIndex + oneBatch >= randomOrder.size()) {
                        toIndex = randomOrder.size();
                    } else {
                        toIndex = fromIndex + oneBatch;
                    }

                    i = toIndex + 1;
                    List<Integer> subOffsets = randomOrder.subList(fromIndex, toIndex);
                    monitor.startNewOffsets(buildOffsets(subOffsets));
                }


            }
        };

        new Thread(receiveThread).start();
        Runnable ackThread = new Runnable() {
            @Override
            public void run() {
                for (Integer integer : randomOrder) {
                    if (integer % 100 == 0) {
                        monitor.offsetDone(new Offset(id, integer), false);
                        ackFailSet.add(integer);
                    } else {
                        monitor.offsetDone(new Offset(id, integer), true);
                    }
                }
            }
        };
        new Thread(ackThread).start();


        Thread.sleep(2500);
//        assertEquals(new HashSet<>(randomOrder), doneSet);

        assertEquals(ackFailSet, failSet);
    }

    @Test
    public void multiThreadMostFail() {

    }

    @Test
    public void multiThreadMostTimeout() {

    }

    private Offset[] buildOffsets(List<Integer> offsets) {
        Offset[] result = new Offset[offsets.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = new Offset(id, offsets.get(i));
        }
        return result;
    }

    private void assertOffsetEqual(Offset offset1, Offset offset2) {
        assertTrue(offset1.getId().equals(offset2.getId())
                && offset1.getOffset() == offset2.getOffset());
    }
*/
}
