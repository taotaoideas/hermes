package com.ctrip.hermes.range.monitor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.range.DefaultRangeMonitor;
import com.ctrip.hermes.storage.range.OffsetRecord;
import com.ctrip.hermes.storage.range.RangeEvent;
import com.ctrip.hermes.storage.range.RangeStatusListener;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.StorageException;

import junit.framework.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultRangeMonitorTest extends ComponentTestCase {
    final String ID = "id";
    final Offset toUpdate = new Offset(ID, 555);


    @Test
    public void testAllSuccess() throws InterruptedException {
        DefaultRangeMonitor monitor = new DefaultRangeMonitor();
        CountDownLatch successLatch = new CountDownLatch(1);
        CountDownLatch failLatch = new CountDownLatch(1);
        int length = 10;

        monitor.addListener(buildListener(successLatch, failLatch, length));
        List<Offset> offsets = buildOffsetList(length);

        monitor.startNewRange(buildOffsetRecord(offsets));
        monitor.offsetDone(buildAckRecord(offsets, Ack.SUCCESS));

        assertTrue(successLatch.await(1, TimeUnit.SECONDS));
        assertFalse(failLatch.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testAllFail() throws InterruptedException {
        DefaultRangeMonitor monitor = new DefaultRangeMonitor();
        CountDownLatch successLatch = new CountDownLatch(1);
        CountDownLatch failLatch = new CountDownLatch(1);
        int length = 10;

        monitor.addListener(buildListener(successLatch, failLatch, length));
        List<Offset> offsets = buildOffsetList(length);

        monitor.startNewRange(buildOffsetRecord(offsets));
        monitor.offsetDone(buildAckRecord(offsets, Ack.FAIL));

        assertTrue(successLatch.await(1, TimeUnit.SECONDS));
        assertTrue(failLatch.await(1, TimeUnit.SECONDS));
    }


    private List<Offset> buildOffsetList(int length) {
        List<Offset> result = new ArrayList<>();
        for (int i = 0; i< length; i ++) {
            result.add(new Offset(ID, i));
        }
        return result;
    }

    private RangeStatusListener buildListener(final CountDownLatch success, final CountDownLatch fail,
                                              final int length) {
        return new RangeStatusListener() {
            @Override
            public void onRangeSuccess(RangeEvent event) throws StorageException {
                assertEquals(event.getRecord().getToBeDone().size(), length);
                assertEquals(event.getRecord().getToUpdate(), toUpdate);
                assertEquals(event.getRecord().getToUpdate().getId(), ID);
                success.countDown();
            }

            @Override
            public void onRangeFail(RangeEvent event) throws StorageException {
                assertEquals(event.getRecord().getToBeDone().size(), length);
                assertEquals(event.getRecord().getToUpdate(), toUpdate);
                assertEquals(event.getRecord().getToUpdate().getId(), ID);
                fail.countDown();
            }
        };
    }


    private OffsetRecord buildOffsetRecord(List<Offset> offsets) {
        return new OffsetRecord(offsets, toUpdate);
    }

    private OffsetRecord buildAckRecord(List<Offset> offsets, Ack ack) {
        OffsetRecord record = new OffsetRecord(offsets, toUpdate);
        record.setAck(ack);
        return record;
    }
}
