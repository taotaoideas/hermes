package com.ctrip.hermes.range.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.range.NewOffsetBitmap;
import com.ctrip.hermes.storage.storage.Offset;

import static org.junit.Assert.assertEquals;

public class OffsetBitmapTest extends ComponentTestCase {

    final String ID = "id";
    final int max = 1 * 10000;
    final AtomicInteger sendCount = new AtomicInteger(0);
    final AtomicInteger ackCount = new AtomicInteger(0);
    final Offset toUpdate = new Offset(ID, 555);

    @Before
    public void init() {
        sendCount.set(0);
        ackCount.set(0);
    }

    /**
     * multiple thread write and read.
     */
    @Test
    public void testNewBitmapPerformance() throws InterruptedException {
        LinkedBlockingQueue<Triple<List<Long>, String, Offset>> doneQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Triple<List<Long>, String, Offset>> failQueue = new LinkedBlockingQueue<>();
        final NewOffsetBitmap newBitmap = new NewOffsetBitmap(doneQueue, failQueue);
        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (sendCount.get() < max) {
                        List<Long> list = new ArrayList<>();
                        list.add((long) (sendCount.incrementAndGet()));
                        newBitmap.putOffset(list.iterator(), ID, toUpdate, new Date().getTime());
                    }
                }
            }).start();
        }

        Thread.sleep(1000); // have to sleep for a while, so that make ack after send.
        for (int j = 0; j < 5; j++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (ackCount.get() < max) {
                        List<Long> list = new ArrayList<>();
                        list.add((long) (ackCount.incrementAndGet()));
                        newBitmap.ackOffset(list.iterator(), Ack.SUCCESS);
                    }
                }
            }).start();
        }

        Thread.sleep(3100);
        List<Long> timeout = newBitmap.getTimeoutAndRemove();

        newBitmap.outputDebugInfo();

        assertEquals(calculateCount(doneQueue) + timeout.size(), max);
    }

    @Test
    public void testPutAndSuccessAck() throws InterruptedException {
        int size = 10;
        LinkedBlockingQueue<Triple<List<Long>, String, Offset>> allQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Triple<List<Long>, String, Offset>> failQueue = new LinkedBlockingQueue<>();
        NewOffsetBitmap bitmap = new NewOffsetBitmap(allQueue, failQueue);
        put(bitmap, size);
        ack(bitmap, 0, size, Ack.SUCCESS);

        Thread.sleep(50);

        assertEquals(calculateCount(allQueue), size);
        assertEquals(calculateCount(failQueue), 0);
        assertEquals(bitmap.getTimeoutAndRemove().size(), 0);
    }

    @Test
    public void testPutAndFailAck() throws InterruptedException {
        int size = 10;
        LinkedBlockingQueue<Triple<List<Long>, String, Offset>> allQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Triple<List<Long>, String, Offset>> failQueue = new LinkedBlockingQueue<>();
        NewOffsetBitmap bitmap = new NewOffsetBitmap(allQueue, failQueue);
        put(bitmap, size);
        ack(bitmap, 0, size, Ack.FAIL);

        Thread.sleep(50);

        assertEquals(calculateCount(allQueue), size);
        assertEquals(calculateCount(failQueue), size);
        assertEquals(bitmap.getTimeoutAndRemove().size(), 0);
    }

    @Test
    public void testPutAndSomeFailAck() throws InterruptedException {
        int size = 10;
        LinkedBlockingQueue<Triple<List<Long>, String, Offset>> allQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Triple<List<Long>, String, Offset>> failQueue = new LinkedBlockingQueue<>();
        NewOffsetBitmap bitmap = new NewOffsetBitmap(allQueue, failQueue);
        put(bitmap, size);
        ack(bitmap, 0, 5, Ack.SUCCESS);
        ack(bitmap, 5, 7, Ack.FAIL);
        ack(bitmap, 7, size, Ack.SUCCESS);

        Thread.sleep(50);

        assertEquals(calculateCount(allQueue), size);
        assertEquals(calculateCount(failQueue), 7 - 5);
        assertEquals(bitmap.getTimeoutAndRemove().size(), 0);
    }

    @Test
    public void testPutAndAllTimeout() throws InterruptedException, IOException {
        int size = 10;
        LinkedBlockingQueue<Triple<List<Long>, String, Offset>> allQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Triple<List<Long>, String, Offset>> failQueue = new LinkedBlockingQueue<>();
        NewOffsetBitmap bitmap = new NewOffsetBitmap(allQueue, failQueue);
        put(bitmap, size);

        Thread.sleep(3200);  // longer than timeout time -- 3000

        assertEquals(calculateCount(allQueue), size);
        assertEquals(calculateCount(failQueue), size);
        assertEquals(bitmap.getTimeoutAndRemove().size(), size);
    }


    private void put(NewOffsetBitmap bitmap, int size) {
        List<Long> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add((long) i);
        }
        bitmap.putOffset(list.iterator(), ID, toUpdate, new Date().getTime());
    }

    private void ack(NewOffsetBitmap bitmap, int start, int end, Ack ack) {
        List<Long> list = new ArrayList<>();
        for (int i = start; i < end; i++) {
            list.add((long) i);
        }
        bitmap.ackOffset(list.iterator(), ack);
    }

    private void outputResult(LinkedBlockingQueue<Triple<List<Long>, String, Offset>> success,
                              LinkedBlockingQueue<Triple<List<Long>, String, Offset>> fail,
                              List<Long> timeout) {
        System.out.println(String.format("Success: %d, Fail: %d, Timeout: %d. Total: %d",
                calculateCount(success), calculateCount(fail), timeout.size(),
                calculateCount(success, fail) + timeout.size()));
    }

    private int calculateCount(LinkedBlockingQueue<Triple<List<Long>, String, Offset>> queue) {
        return calculateCount(queue, 0);
    }

    private int calculateCount(LinkedBlockingQueue<Triple<List<Long>, String, Offset>> list, int sum) {
        return sumSize(list, sum);
    }

    private int calculateCount(LinkedBlockingQueue<Triple<List<Long>, String, Offset>> q1,
                               LinkedBlockingQueue<Triple<List<Long>, String, Offset>> q2) {
        return calculateCount(q1, calculateCount(q2));
    }

    private int sumSize(LinkedBlockingQueue<Triple<List<Long>, String, Offset>> list, int sum) {
        for (Triple<List<Long>, String, Offset> triple : list) {
            sum += triple.getFirst().size();
        }
        return sum;
    }
}
