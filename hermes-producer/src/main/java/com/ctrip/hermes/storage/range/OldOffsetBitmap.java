package com.ctrip.hermes.storage.range;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.storage.Offset;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.googlecode.javaewah.EWAHCompressedBitmap;

/**
 * 旧的实现，使用一个整体的Bitmap来记录所有的offset使用情况。
 */
public class OldOffsetBitmap {

    final long TIMEOUT_THRESHOLD = 3 * 1000; // 3s
    final long CLEAN_THRESHOLD = 10 * 1000; // 10s

    static long lastCleanTime = -1;
    Semaphore semaphore = new Semaphore(1);
    final Object timeoutOffsetMapLock = new Object();

    // 1. 由于EWAHCompressedBitmap是数字的，所以需要Map<int, Offset>的映射表，大多是insert和remove操作。
    ConcurrentHashMap<Long, Offset> offsetMap = new ConcurrentHashMap<>();
    EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf();
    EWAHCompressedBitmap successMap = EWAHCompressedBitmap.bitmapOf();
    EWAHCompressedBitmap failMap = EWAHCompressedBitmap.bitmapOf();
    // 2. timeout使用Map<List<Offset>, long>的结构，每次遍历检查timeout的Offset
    ArrayListMultimap<Long /*timestamp*/, List<Long> /*offsets list*/> timeoutOffsetMap = ArrayListMultimap.create();

    // 3. 解决offset是long超过bitmap的int范围的问题
    public void putOffset(List<Offset> offsetList, long timestamp) {
        cleanSelf();

        List<Long> offsetsInList = Lists.newArrayList();
        for (Offset offset : offsetList) {
            offsetMap.put(offset.getOffset(), offset);
            sendMap.set((int) offset.getOffset());
            offsetsInList.add(offset.getOffset());
        }
            timeoutOffsetMap.put(timestamp, offsetsInList);
    }

    public void ackOffset(List<Offset> offsetList, Ack ack) {
        cleanSelf();

        switch (ack) {
            case SUCCESS:
                for (Offset offset : offsetList) {
                    sendMap.clear((int) offset.getOffset());
                    successMap.set((int) offset.getOffset());
                }
                break;
            case FAIL:
                for (Offset offset : offsetList) {
                    sendMap.clear((int) offset.getOffset());
                    failMap.set((int) offset.getOffset());
                }
                break;
        }
    }

    // need synchronized ?
    public List<OffsetRecord> getAndRemoveSuccess() {
        List<Integer> successOnes = successMap.toList();
        Map<Long, Offset> newMap = getMapFromBitmap(offsetMap, successOnes);
        // clear successMap!
        successMap.clear();

        List<OffsetRecord> offsetRecordsList = buildContinuous(new ArrayList<>(newMap.values()));
        return offsetRecordsList;
    }

    private Map<Long, Offset> getMapFromBitmap(Map<Long, Offset> offsetMap, final List<Integer> successOnes) {

        Map<Long, Offset> resultMap = new HashMap<>();

        for (Map.Entry<Long, Offset> entry : offsetMap.entrySet()) {
            if (successOnes.contains(entry.getKey().intValue())) {
                resultMap.put(entry.getKey(), entry.getValue());
            }
        }

        return resultMap;
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
        OldOffsetBitmap bitmap = new OldOffsetBitmap();


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
