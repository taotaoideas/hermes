package com.ctrip.hermes.broker.storage.range;

import java.util.*;

import com.ctrip.hermes.broker.storage.message.Ack;
import com.ctrip.hermes.broker.storage.storage.Offset;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.javaewah.EWAHCompressedBitmap;

public class OffsetBitmap {

    final long TIMEOUT_THRESHOLD = 3 * 1000; // 3s
    final long CLEAN_THRESHOLD = 10 * 1000; // 10s

    long lastCleanTime = -1;

    // 1. 由于EWAHCompressedBitmap是数字的，所以需要Map<int, Offset>的映射表，大多是insert和remove操作。
    LinkedHashMap<Long, Offset> offsetMap = Maps.newLinkedHashMap();
    EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf();
    EWAHCompressedBitmap successMap = EWAHCompressedBitmap.bitmapOf();
    EWAHCompressedBitmap failMap = EWAHCompressedBitmap.bitmapOf();
    // 2. timeout使用Map<List<Offset>, long>的结构，每次遍历检查timeout的Offset
    TreeMap<Long /*timestamp*/, List<Long> /*offsets list*/> timeoutTreeMap = Maps.newTreeMap();

    // 3. 解决offset是long超过bitmap的int范围的问题
    public void putOffset(List<Offset> offsetList, long timestamp) {
        cleanSelf();

        List<Long> offsetsInList = Lists.newArrayList();
        for (Offset offset : offsetList) {
            offsetMap.put(offset.getOffset(), offset);
            sendMap.set((int) offset.getOffset());
            offsetsInList.add(offset.getOffset());
        }
        timeoutTreeMap.put(timestamp, offsetsInList);
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
        return Maps.filterKeys(offsetMap, new Predicate<Long>() {
            @Override
            public boolean apply(Long input) {
                return successOnes.contains(input.intValue());
            }
        });
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

    public List<OffsetRecord> getAndRemoveTimeout() {
        cleanSelf();

        long nowTime = new Date().getTime();
        List<Offset> timeoutOffset = Lists.newArrayList();
        List<Long> allOffsetInBitmap = Lists.newArrayList();
        for (Map.Entry<Long, List<Long>> entry : timeoutTreeMap.entrySet()) {
            long timestamp = entry.getKey();
            if (nowTime - timestamp > TIMEOUT_THRESHOLD) {
                List<Long> offsetsInBitmap = entry.getValue();
                allOffsetInBitmap.addAll(offsetsInBitmap);
            } else {
                break;
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
        if (shouldDoClean()) {
            //todo:
            // clean sendMap
            // clean successMap
            // clean failMap

            // clean offsetMap
            // clean timeoutTreeMap => put into oldMap( max capacity 10000),

            lastCleanTime = new Date().getTime(); // assign to new time, just in case cleanSelf() takes too much time.
        }
    }

    private boolean shouldDoClean() {
        return lastCleanTime == -1 || (new Date().getTime() - lastCleanTime) > CLEAN_THRESHOLD;
    }
}
