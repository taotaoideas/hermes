package com.ctrip.hermes.range.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.range.NewOffsetBitmap;
import com.ctrip.hermes.storage.range.OffsetRecord;
import com.ctrip.hermes.storage.range.OldOffsetBitmap;
import com.ctrip.hermes.storage.storage.Offset;

import static org.junit.Assert.assertEquals;

public class OffsetBitmapTest extends ComponentTestCase {

    final String ID = "id";
    final int max = 1 * 10000;
    final AtomicInteger sendCount = new AtomicInteger(0);
    final AtomicInteger ackCount = new AtomicInteger(0);

    OldOffsetBitmap oldBitmap = new OldOffsetBitmap();


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
        final NewOffsetBitmap newBitmap = new NewOffsetBitmap();
        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (sendCount.get() < max) {
                        List<Offset> list = new ArrayList<>();
                        list.add(new Offset(ID, sendCount.incrementAndGet()));
                        newBitmap.putOffset(list, new Date().getTime());
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
                        List<Offset> list = new ArrayList<>();
                        list.add(new Offset(ID, ackCount.incrementAndGet()));
                        newBitmap.ackOffset(list, Ack.SUCCESS);
                    }
                }
            }).start();
        }

        Thread.sleep(3100);
        List<OffsetRecord> success = newBitmap.getAndRemoveSuccess();
        List<OffsetRecord> fail = newBitmap.getAndRemoveFail();
        List<OffsetRecord> timeout = newBitmap.getTimeoutAndRemove();

        newBitmap.outputDebugInfo();
        outputResult(success, fail, timeout);
        assertEquals(calculateCount(success, fail, timeout), max);
    }

    @Test
    public void testPutAndSuccessAck() throws InterruptedException {
        int size = 10;
        NewOffsetBitmap bitmap = new NewOffsetBitmap();
        put(bitmap, size);
        ack(bitmap, 0, size, Ack.SUCCESS);

        Thread.sleep(50);

        assertEquals(calculateCount(bitmap.getAndRemoveSuccess()), size);
        assertEquals(calculateCount(bitmap.getAndRemoveFail()), 0);
        assertEquals(calculateCount(bitmap.getTimeoutAndRemove()), 0);
    }

    @Test
    public void testPutAndFailAck() throws InterruptedException {
        int size = 10;
        NewOffsetBitmap bitmap = new NewOffsetBitmap();
        put(bitmap, size);
        ack(bitmap, 0, size, Ack.FAIL);

        Thread.sleep(50);

        assertEquals(calculateCount(bitmap.getAndRemoveSuccess()), 0);
        assertEquals(calculateCount(bitmap.getAndRemoveFail()), size);
        assertEquals(calculateCount(bitmap.getTimeoutAndRemove()), 0);

    }

    @Test
    public void testPutAndAllTimeout() throws InterruptedException, IOException {
        int size = 10;
        NewOffsetBitmap bitmap = new NewOffsetBitmap();
        put(bitmap, size);

        Thread.sleep(3200);  // longer than timeout time -- 3000

        assertEquals(calculateCount(bitmap.getAndRemoveSuccess()), 0);
        assertEquals(calculateCount(bitmap.getAndRemoveFail()), 0);
        assertEquals(calculateCount(bitmap.getTimeoutAndRemove()), size);
    }


    private void put(NewOffsetBitmap bitmap, int size) {
        List<Offset> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(new Offset(ID, i));
        }
        bitmap.putOffset(list, new Date().getTime());
    }

    private void ack(NewOffsetBitmap bitmap, int start, int end, Ack ack) {
        List<Offset> list = new ArrayList<>();
        for (int i = start; i < end; i++) {
            list.add(new Offset(ID, i));
        }
        bitmap.ackOffset(list, ack);
    }

    private void outputResult(List<OffsetRecord> success, List<OffsetRecord> fail, List<OffsetRecord> timeout) {
        System.out.println(String.format("Success: %d, Fail: %d, Timeout: %d. Total: %d",
                calculateCount(success), calculateCount(fail), calculateCount(timeout),
                calculateCount(success, fail, timeout)));
    }

    private int calculateCount(List<OffsetRecord> list) {
        return calculateCount(list, 0);
    }

    private int calculateCount(List<OffsetRecord> list, int sum) {
        return sumSize(list, sum);
    }

    private int sumSize(List<OffsetRecord> list, int sum) {
        for (OffsetRecord offsetRecord : list) {
            sum += offsetRecord.getToBeDone().size();
        }
        return sum;
    }

    private int calculateCount(List<OffsetRecord> a, List<OffsetRecord> b, List<OffsetRecord> c) {
        return calculateCount(a, calculateCount(b, calculateCount(c)));
    }
}
