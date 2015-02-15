package com.ctrip.hermes.broker.storage.range;


import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.hermes.broker.storage.message.Ack;
import com.ctrip.hermes.broker.storage.storage.Offset;
import com.ctrip.hermes.broker.storage.storage.StorageException;

public class DefaultRangeMonitorPerformanceTest {


    final int totalLength = 10000;
    final int maxBatchLength = 1000;

    final String offsetId = "DefaultRangeMonitorPerformanceTest";

    @Test
    public void runSendAndAck() throws InterruptedException {
        List<Integer> offsetIntList = new ArrayList<>(totalLength);
        for (int i = 0; i < totalLength; i++) {
            offsetIntList.add(i);
        }
        Collections.shuffle(offsetIntList);

        // build List<OffsetRecord>
        List<OffsetRecord> recordList = new ArrayList<>();
        for (int start = 0; start < totalLength; start++) {
            int randomBatchSize = new Random().nextInt(maxBatchLength);
            int end = (start + randomBatchSize) > totalLength ? totalLength : start + randomBatchSize;

            List<Offset> offsetList = new ArrayList<>();
            for (int i = start; i < end; i++) {
                offsetList.add(new Offset(offsetId, (long) i));
            }

            OffsetRecord record = new OffsetRecord(offsetList, null);
            recordList.add(record);
            start = end + 1;
        }

        // now start, 先简化的发了再ack
        final CountDownLatch latch = new CountDownLatch(recordList.size());
        final Set<Offset> receivedOffset = new HashSet<>();

        RangeMonitor rangeMonitor = new DefaultRangeMonitor();
        rangeMonitor.addListener(new RangeStatusListener() {
            @Override
            public void onRangeSuccess(RangeEvent event) throws StorageException {
                receivedOffset.addAll(event.getRecord().getToBeDone());
                latch.countDown();
            }

            @Override
            public void onRangeFail(RangeEvent event) throws StorageException {
                receivedOffset.addAll(event.getRecord().getToBeDone());
                latch.countDown();
            }
        });

        for (OffsetRecord record : recordList) {
            rangeMonitor.startNewRange(record);
        }

        for (OffsetRecord record : recordList) {
            Ack ack = new Random().nextInt(2) == 1 ? Ack.SUCCESS : Ack.FAIL;
            rangeMonitor.offsetDone(record, ack);
        }

        Thread.sleep(3000);
        System.out.println("Send Offset: " + offsetIntList.size() + "\nReceive Offset: " + receivedOffset.size());
        Assert.assertEquals(receivedOffset.size(), offsetIntList.size());
    }
}