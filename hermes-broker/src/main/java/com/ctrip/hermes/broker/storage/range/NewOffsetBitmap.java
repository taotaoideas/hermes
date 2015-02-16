package com.ctrip.hermes.broker.storage.range;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import com.ctrip.hermes.broker.storage.message.Ack;
import com.ctrip.hermes.broker.storage.storage.Offset;
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
           // 根据success 和fail的都取完了，则可以从  batchTimestampMap中remove掉了。
        }
    }

    final long TIMEOUT_THRESHOLD = 3 * 1000; // 3s
    final long CLEAN_THRESHOLD = 10 * 1000; // 10s

    static long lastCleanTime = -1;
    Semaphore semaphore = new Semaphore(1);

    ArrayListMultimap<Long /*timestamp*/, Batch /*offsets list*/> batchTimestampMap = ArrayListMultimap.create();

    ConcurrentHashMap<Long, Offset> offsetMap = new ConcurrentHashMap<>();
    EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf();
    EWAHCompressedBitmap successMap = EWAHCompressedBitmap.bitmapOf();
    EWAHCompressedBitmap failMap = EWAHCompressedBitmap.bitmapOf();
    // 2. timeout使用Map<List<Offset>, long>的结构，每次遍历检查timeout的Offset
    ArrayListMultimap<Long /*timestamp*/, List<Long> /*offsets list*/> timeoutOffsetMap = ArrayListMultimap.create();

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

        List<Offset> doneOffsetList = new ArrayList<>();
        Iterator<Map.Entry<Long, Batch>> iterator = batchTimestampMap.entries().iterator();
        while (iterator.hasNext()) {
            Batch batch = iterator.next().getValue();
            if (batch.isAllDone()) {
                doneOffsetList.addAll(batch.getSuccessOffsets());
                // remove success
                if (batch.canBeRemoved()) {
                    iterator.remove();
                }
            }
        }

        List<OffsetRecord> offsetRecordsList = buildContinuous(doneOffsetList);
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
        List<Integer> failOnes = failMap.toList();
        Map<Long, Offset> newMap = getMapFromBitmap(offsetMap, failOnes);
        // clear failMap!
        failMap.clear();

        return buildContinuous(new ArrayList<>(newMap.values()));
    }

    //todo: do remove timeout
    public List<OffsetRecord> getTimeoutAndRemove() {
        cleanSelf();

        long nowTime = new Date().getTime();
        List<Offset> timeoutOffset = Lists.newArrayList();
        List<Long> allOffsetInBitmap = Lists.newArrayList();

        for (Long ts : timeoutOffsetMap.keySet()) {
            if (nowTime - ts > TIMEOUT_THRESHOLD) {
                List<List<Long>> list = timeoutOffsetMap.get(ts);
                for (List<Long> offsetsInBitmap : list) {
                    allOffsetInBitmap.addAll(offsetsInBitmap);
                }
            }
        }
        for (Long offset : allOffsetInBitmap) {
            if (sendMap.get(offset.intValue())) {
                timeoutOffset.add(offsetMap.get(offset));
            }
        }

        // todo: merge data with oldMap (generated in cleanSelf())
        return buildContinuous(timeoutOffset);
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

                //  debug
                System.out.println("TimeoutTreeMap Before Clean, Total Size: " + timeoutOffsetMap.size());
                //  debug
                Iterator<Long> iterator = timeoutOffsetMap.keySet().iterator();

                while (iterator.hasNext()) {

                    Long timestamp = iterator.next();
                    if (nowTime - timestamp > CLEAN_THRESHOLD) {
                        for (List<Long> offsets : timeoutOffsetMap.get(timestamp)) {
                            oldOffsetPointer.addAll(offsets);
                        }
                        // clean timeoutOffsetMap
                        iterator.remove();
                    }
                }

                //  debug
                System.out.println("TimeoutTreeMap After Clean, Total Size: " + timeoutOffsetMap.size());
                //  debug

                for (Long offset : oldOffsetPointer) {
                    // 针对bitmap的clear可优化成bitmap的nor操作
                    // clean sendMap
                    sendMap.clear(offset.intValue());  // todo: 实际上没有清理掉
                    // clean successMap
                    successMap.clear(offset.intValue());
                    // clean failMap
                    failMap.clear(offset.intValue());
                    // clean offsetMap
                    offsetMap.remove(offset);  // todo: 实际没有清理掉
                }

                // output clean result
                System.out.println(String.format("CLEAN RESULT:\n" +
                                "\tOld Offsets (Cleaned Offsets): %d(items)\n" +
                                "\tSendBitMap.sizeInBytes(): %d, SuccessBitMap.sizeInBytes(): %d, FailBitMap.sizeInBytes(): %d\n" +
                                "\tOffsetMap: %d(items), TimeoutTreeMap: %d(items)",
                        oldOffsetPointer.size(), sendMap.sizeInBytes(), successMap.sizeInBytes(), failMap.sizeInBytes(),
                        offsetMap.size(), timeoutOffsetMap.size()));
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
