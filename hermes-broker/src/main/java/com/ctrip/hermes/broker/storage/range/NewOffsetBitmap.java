package com.ctrip.hermes.broker.storage.range;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.ctrip.hermes.broker.storage.message.Ack;
import com.ctrip.hermes.broker.storage.storage.Offset;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.googlecode.javaewah.EWAHCompressedBitmap;

/**
 * 有两种使用bitmap的策略：1),将所有offset存于一个bitmap中（因为，前提假设是会不重复的）
 * 2),基于每次batch将多个offset存放于各独立的bitmap中。
 */
public class NewOffsetBitmap {

    private class Batch {
        Map<Long, Offset> offsetMap = new HashMap<>();
        EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf();
        EWAHCompressedBitmap successMap = EWAHCompressedBitmap.bitmapOf();
        EWAHCompressedBitmap failMap = EWAHCompressedBitmap.bitmapOf();

        public Batch(List<Offset> offsetList) {
            for (Offset offset : offsetList) {
                offsetMap.put(offset.getOffset(), offset);
                sendMap.set((int) offset.getOffset());
            }
        }

        public void ackOffset(Offset offset, Ack ack) {
            switch (ack) {
                case SUCCESS:
                    sendMap.clear((int) offset.getOffset());
                    successMap.set((int) offset.getOffset());
                    break;
                case FAIL:
                    sendMap.clear((int) offset.getOffset());
                    failMap.set((int) offset.getOffset());
                    break;
            }
        }

        public List<Offset> getAllOffset() {
            // clean maps
        }

        public boolean canBeRemoved() {
            // 根据isAllDone()之后，且success 和fail的都取完了，则可以从  batchTimestampMap中remove掉了。
        }
    }

    final long TIMEOUT_THRESHOLD = 3 * 1000; // 3s
    final long CLEAN_THRESHOLD = 10 * 1000; // 10s

    static long lastCleanTime = -1;
    Semaphore semaphore = new Semaphore(1);

    ArrayListMultimap<Long /*timestamp*/, Batch /*offsets list*/> batchTimestampMap = ArrayListMultimap.create();

    // maxSize is OK? If send too fast and ack too slow, this capacity will clean "old" data by LRU.
    Cache<Long, Offset> remainSuccessOffsetCache = CacheBuilder.newBuilder()
            .maximumSize(10 * 1000)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    Cache<Long, Offset> remainFailOffsetCache = CacheBuilder.newBuilder()
            .maximumSize(10 * 1000)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();


    public void putOffset(List<Offset> offsetList, long timestamp) {
        cleanSelf();

        batchTimestampMap.put(timestamp, new Batch(offsetList));
    }

    public void ackOffset(List<Offset> offsetList, Ack ack) {
        cleanSelf();

        for (Offset offset : offsetList) {
            Batch batch = findBatch(batchTimestampMap);
            batch.ackOffset(offset, ack);
        }
    }

    public List<OffsetRecord> getAndRemoveSuccess() {
        List<Offset> successOffsetList = new ArrayList<>();
        Iterator<Map.Entry<Long, Batch>> iterator = batchTimestampMap.entries().iterator();
        while (iterator.hasNext()) {
            Batch batch = iterator.next().getValue();
            if (batch.isAllDone()) {
                successOffsetList.addAll(batch.getSuccessOffsets());
                // remove success
                if (batch.canBeRemoved()) {
                    iterator.remove();
                }
            }
        }

        successOffsetList.addAll(remainSuccessOffsetCache.asMap().values());
        remainSuccessOffsetCache.cleanUp();

        List<OffsetRecord> offsetRecordsList = buildContinuous(successOffsetList);
        return offsetRecordsList;
    }

    private List<OffsetRecord> buildContinuous(List<Offset> offsets) {
        // now: only one OffsetRecord;
        if (offsets.size() > 0) {
            return Arrays.asList(new OffsetRecord(offsets, offsets.get(0)));
        } else {
            return Lists.newArrayList();
        }
    }

    public List<OffsetRecord> getAndRemoveFail() {
        List<Offset> failOffsetList = new ArrayList<>();
        Iterator<Map.Entry<Long, Batch>> iterator = batchTimestampMap.entries().iterator();
        while (iterator.hasNext()) {
            Batch batch = iterator.next().getValue();
            if (batch.isAllDone()) {
                failOffsetList.addAll(batch.getFailOffests());
                // remove success
                if (batch.canBeRemoved()) {
                    iterator.remove();
                }
            }
        }

        failOffsetList.addAll(remainFailOffsetCache.asMap().values());
        remainFailOffsetCache.invalidateAll();

        List<OffsetRecord> offsetRecordsList = buildContinuous(failOffsetList);
        return offsetRecordsList;
    }


    public List<OffsetRecord> getTimeoutAndRemove() {
        cleanSelf();


        long nowTime = new Date().getTime();
        List<Offset> successOffsetList = new ArrayList<>();
        List<Offset> failOffsetList = new ArrayList<>();
        List<Offset> timeoutOffsetList = new ArrayList<>();
        Iterator<Map.Entry<Long, Batch>> iterator = batchTimestampMap.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Batch> entry = iterator.next();
            Long ts = entry.getKey();
            Batch batch = entry.getValue();
            if (nowTime - ts > TIMEOUT_THRESHOLD) { // it is timeout
                successOffsetList.addAll(batch.getSuccessOffsetst());
                failOffsetList.addAll(batch.getFailOffsets());
                timeoutOffsetList.add(batch.getTimetoutOffsets());
            }
        }

        remainSuccessOffsetCache.putAll(successOffsetList);
        remainFailOffsetList.addAll(failOffsetList);

        // todo: merge data with oldMap (generated in cleanSelf())
        return buildContinuous(timeoutOffsetList);
    }

    /**
     * avoid OOM, should clean self: remove "old" data,
     * even if no put or ack or getAndRemove~ for long time.
     */
    private void cleanSelf() {
        if (shouldDoClean() && semaphore.tryAcquire()) {
            try {
                lastCleanTime = new Date().getTime(); // assign to new time, just in case cleanSelf() takes too much time.

                final long nowTime = new Date().getTime();

                List<Long> oldOffsetPointer = new ArrayList<>();

                Iterator<Map.Entry<Long, Batch>> iterator = batchTimestampMap.entries().iterator();

                while (iterator.hasNext()) {

                    Map.Entry<Long, Batch> entry = iterator.next();
                    Long timestamp = entry.getKey();

                    if (nowTime - timestamp > CLEAN_THRESHOLD) {
                        Batch batch = entry.getValue();
                        // todo: 2.16: 继续搞定clean self.
                        for (List<Long> offsets : batch.) {
                            oldOffsetPointer.addAll(offsets);
                        }
                        // clean timeoutOffsetMap
                        iterator.remove();
                    }
                }

            } finally {
                semaphore.release();
            }
        }
    }

    private boolean shouldDoClean() {
        return lastCleanTime == -1 || (new Date().getTime() - lastCleanTime) > CLEAN_THRESHOLD;
    }

    public static void main(String[] args) throws InterruptedException {
        NewOffsetBitmap bitmap = new NewOffsetBitmap();


        for (int i = 0; i < 10; i++) {
            List<Offset> offsetList = new ArrayList<>();
            for (int j = 0; j < 1000; j++) {
                offsetList.add(new Offset("id", j));
            }

            bitmap.putOffset(offsetList, new Date().getTime());
            Thread.sleep(1000);
        }

        for (int i = 0; i < 10; i++) {
            List<Offset> offsetList = new ArrayList<>();
            for (int j = 0; j < 1000; j++) {
                offsetList.add(new Offset("id", j));
            }
            bitmap.ackOffset(offsetList, Ack.SUCCESS);
            Thread.sleep(1000);
        }
    }

}
