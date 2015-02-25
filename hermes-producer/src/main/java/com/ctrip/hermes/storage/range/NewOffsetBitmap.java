package com.ctrip.hermes.storage.range;

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
 * 每次batch将多个offset存放于各独立的bitmap中。
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

        public List<Offset> popSuccessOffsets() {
            List<Offset> result = new ArrayList<>();
            for (Integer index : successMap.toList()) {
                result.add(offsetMap.get((long)index));
            }

            successMap.clear();
            return result;
        }

        public List<Offset> popFailOffsets() {
            List<Offset> result = new ArrayList<>();
            for (Integer index : failMap.toList()) {
                result.add(offsetMap.get((long)index));
            }

            failMap.clear();
            return result;
        }

        public List<Offset> popTimeoutOffsets() {
            // sendMap and ((not) ( successMap or failMap)) = remaining = timeoutMap
            EWAHCompressedBitmap timeoutMap = sendMap.andNot(successMap.or(failMap));
            List<Offset> result = new ArrayList<>();

            for (Integer index : timeoutMap.toList()) {
                result.add(offsetMap.get((long)index));
            }
            // here is no need to clear related offsets in sendMap, as this node will be removed soon.
            return result;
        }

        public boolean canBeRemoved() {
            // 根据isAllDone()之后，且success 和fail的都取完了，则可以从  batchTimestampMap中remove掉了。
            boolean canBeRemoved = false;
            if (isAllDone()) {
                if (successMap.isEmpty() && failMap.isEmpty()) {
                    canBeRemoved = true;
                }
            }
            return canBeRemoved;
        }

        public boolean contains(Offset offset) {
            // 直接从sendMap取，而不是从offsetMap。可以避免发了一个，后续又重发导致offsetMap里面会有重复的情况，
            // 因为sendMap ack会清空，只有后一个Batch的sendMap有该值。
            return sendMap.get((int)offset.getOffset());
        }

        /**
         * @return is all offsets of sendMap has been acked (whether success of fail)
         */
        public boolean isAllDone() {
            return sendMap.isEmpty();
        }
    }

    final long TIMEOUT_THRESHOLD = 3 * 1000; // 3s

    static long lastCleanTime = -1;
    Semaphore semaphore = new Semaphore(1);

    ArrayListMultimap<Long /*timestamp*/, Batch /*offsets list*/> batchTimestampMap = ArrayListMultimap.create();

    // maxSize is OK? If send too fast and ack too slow, this capacity will clean "old" data by LRU.
    // use Offset as key, to avoid the overlap on Long(timestamp)
    Cache<Offset, Long> remainSuccessOffsetCache = CacheBuilder.newBuilder()
            .maximumSize(10 * 1000)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    Cache<Offset, Long> remainFailOffsetCache = CacheBuilder.newBuilder()
            .maximumSize(10 * 1000)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    Cache<Offset, Long> remainTimeoutOffsetCache = CacheBuilder.newBuilder()
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
            Batch batch = findBatch(batchTimestampMap, offset);
            if (null != batch) {
                batch.ackOffset(offset, ack);
            }
        }
    }

    /**
     * 根据指定的Offset，找到相应的Batch。
     *
     * @param batchMap ：Batch的map
     * @param offset   : 指定的offset
     * @return 相应的batch，若未找到则返回null
     */
    private Batch findBatch(ArrayListMultimap<Long, Batch> batchMap, Offset offset) {
        // Todo:如何更快的找到对应的offset
        for (Batch batch : batchMap.values()) {
            if (batch.contains(offset)) {
                return batch;
            }
        }
        return null;
    }

    public List<OffsetRecord> getAndRemoveSuccess() {
        List<Offset> successOffsetList = new ArrayList<>();
        Iterator<Map.Entry<Long, Batch>> iterator = batchTimestampMap.entries().iterator();
        while (iterator.hasNext()) {
            Batch batch = iterator.next().getValue();
            if (batch.isAllDone()) {
                successOffsetList.addAll(batch.popSuccessOffsets());
                // remove success
                if (batch.canBeRemoved()) {
                    iterator.remove();
                }
            }
        }

        successOffsetList.addAll(remainSuccessOffsetCache.asMap().keySet());
        remainSuccessOffsetCache.cleanUp();

        return buildContinuous(successOffsetList);
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
                failOffsetList.addAll(batch.popFailOffsets());
                // remove success
                if (batch.canBeRemoved()) {
                    iterator.remove();
                }
            }
        }

        failOffsetList.addAll(remainFailOffsetCache.asMap().keySet());
        remainFailOffsetCache.invalidateAll();

        return buildContinuous(failOffsetList);
    }


    public List<OffsetRecord> getTimeoutAndRemove() {
        cleanSelf();

        List<Offset> remainTimeoutOffsetList = new ArrayList<>(remainTimeoutOffsetCache.asMap().keySet());
        return buildContinuous(remainTimeoutOffsetList);
    }

    /**
     * This function does these things:
     * 1. find timeout Batch,
     * 2. then find all success offsets into remainSuccessOffsetCache,
     * 3. then all failed offsets
     * 4. then all timeout offsets.
     */
    private void cleanSelf() {
        if (shouldDoClean() && semaphore.tryAcquire()) {
            try {
                lastCleanTime = new Date().getTime(); // assign to new time, just in case cleanSelf() takes too much time.

                final long nowTime = new Date().getTime();

                Iterator<Map.Entry<Long, Batch>> iterator = batchTimestampMap.entries().iterator();

                // 可根据采用Timestamp的TreeMap来优化筛选过程，就不用挨个遍历
                while (iterator.hasNext()) {
                    Map.Entry<Long, Batch> entry = iterator.next();
                    Long timestamp = entry.getKey();

                    if (nowTime - timestamp > TIMEOUT_THRESHOLD) {
                        Batch batch = entry.getValue();
                        remainSuccessOffsetCache.putAll(buildOffsetMap(batch.popSuccessOffsets(), entry.getKey()));
                        remainFailOffsetCache.putAll(buildOffsetMap(batch.popFailOffsets(), entry.getKey()));
                        remainTimeoutOffsetCache.putAll(buildOffsetMap(batch.popTimeoutOffsets(), entry.getKey()));

                        iterator.remove();
                    }
                }
            } finally {
                semaphore.release();
            }
        }
    }


    private Map<Offset, Long> buildOffsetMap(List<Offset> offsetList, Long timestamp) {
        Map<Offset, Long> result = new HashMap<>();
        for (Offset offset : offsetList) {
            result.put(offset, timestamp);
        }
        return result;
    }


    private boolean shouldDoClean() {
        return lastCleanTime == -1 || (new Date().getTime() - lastCleanTime) > TIMEOUT_THRESHOLD;
    }
}
