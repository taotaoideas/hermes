package com.ctrip.hermes.storage.range;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.storage.Offset;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.googlecode.javaewah.EWAHCompressedBitmap;

/**
 * 每次batch将多个offset存放于各独立的bitmap中。
 */
public class NewOffsetBitmap {

    Logger log = LogManager.getLogger(NewOffsetBitmap.class);


    ExecutorService service = Executors.newSingleThreadExecutor();
    LinkedBlockingQueue<List<Long>> successQueue;
    LinkedBlockingQueue<List<Long>> failQueue;

    public NewOffsetBitmap(LinkedBlockingQueue<List<Long>> successQueue,
                           LinkedBlockingQueue<List<Long>> failQueue) {
        this.successQueue = successQueue;
        this.failQueue = failQueue;

        // clean timer
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    cleanSelf();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 1550);   // 2* 1550 > TIMEOUT_THRESHOLD = 3000, so that will trigger clean every tow times.
    }

    final long TIMEOUT_THRESHOLD = 3 * 1000; // 3s

    static long lastCleanTime = -1;

    Multimap<Long /*timestamp*/, Batch /*offsets list*/> batchTimestampMap =
            Multimaps.synchronizedListMultimap(ArrayListMultimap.<Long, Batch>create());

    // maxSize is OK? If send too fast and ack too slow, this capacity will clean "old" data by LRU.
    // use Offset as key, to avoid the overlap on Long(timestamp)
    Cache<Offset, Long> remainSuccessOffsetCache = CacheBuilder.newBuilder()
            .maximumSize(20 * 1000).expireAfterWrite(10, TimeUnit.MINUTES).build();
    Cache<Offset, Long> remainFailOffsetCache = CacheBuilder.newBuilder()
            .maximumSize(20 * 1000).expireAfterWrite(10, TimeUnit.MINUTES).build();
    Cache<Long /*offset*/, Long /*timestamp*/> remainTimeoutOffsetCache = CacheBuilder.newBuilder()
            .maximumSize(20 * 1000).expireAfterWrite(10, TimeUnit.MINUTES).build();

    long putTimer = 0, ackTimer = 0, cleanTimer = 0;
    int putCount = 0, ackCount = 0, cleanCount = 0;

    private Runnable putOffsetRunnable(final List<Long> offsetList, final long timestamp) {
        return new Runnable() {
            @Override
            public void run() {
                long startTime = new Date().getTime();
                batchTimestampMap.put(timestamp, new Batch(offsetList.iterator(), timestamp));

                putTimer += new Date().getTime() - startTime;
                putCount++;
            }
        };
    }

    private Runnable ackOffsetRunnable(final List<Long> offsetList, final Ack ack) {
        return new Runnable() {
            @Override
            public void run() {
                long startTime = new Date().getTime();
                for (Long offset : offsetList) {
                    Batch batch = findBatch(batchTimestampMap, offset);
                    if (null != batch) {
                        batch.ackOffset(offset, ack);
                        // put into queue, if all batch is done (weather success or fail)
                        if (batch.isAllDone()) {
                            try {
                                successQueue.put(batch.popSuccessOffsets());
                                failQueue.put(batch.popFailOffsets());
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                ackTimer += new Date().getTime() - startTime;
                ackCount++;
            }
        };
    }

    private Runnable cleanRunnable() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    cleanSelf();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public void outputDebugInfo() {
        log.info(String.format("PUT: %d(ms)/%d, ACK: %d(ms)/%d, Clean: %d(ms)/%d.",
                putTimer, putCount, ackTimer, ackCount, cleanTimer, cleanCount));
    }

    public void putOffset(List<Long> offsetList, long timestamp) {
        try {
            putCleanTask();
            service.submit(putOffsetRunnable(offsetList, timestamp));
        } catch (InterruptedException e) {
            log.info("NewOffsetBitmap: putOffset() is Interrupted!");
        }
    }

    public void ackOffset(List<Long> offsetList, Ack ack) {
        try {
            putCleanTask();
            service.submit(ackOffsetRunnable(offsetList, ack));
        } catch (InterruptedException e) {
            log.info("NewOffsetBitmap: ackOffset() is Interrupted!");
        }
    }

    private void putCleanTask() throws InterruptedException {
        if (shouldDoClean()) {
            service.submit(cleanRunnable());
        }
    }

    /**
     * 根据指定的Offset，找到相应的Batch。
     *
     * @param batchMap ：Batch的map
     * @param offset   : 指定的offset
     * @return 相应的batch，若未找到则返回null
     */
    private Batch findBatch(Multimap<Long, Batch> batchMap, Long offset) {
        // Todo:如何更快的找到对应的offset
        for (Batch batch : batchMap.values()) {
            if (batch.contains(offset)) {
                return batch;
            }
        }
        return null;
    }

    /*public List<OffsetRecord> getAndRemoveSuccess() {
        List<Offset> successOffsetList = new ArrayList<>();
        Iterator<Map.Entry<Long, Batch>> iterator = batchTimestampMap.entries().iterator();
        while (iterator.hasNext()) {
            Batch batch = iterator.next().getValue();
            if (batch != null && batch.isAllDone()) {
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
    }*/


    public List<Long> getTimeoutAndRemove() {
        try {
            putCleanTask();
        } catch (InterruptedException e) {
            log.info("NewOffsetBitmap: getTimeoutAndRemove() is Interrupted!");
        }

        List<Long> remainTimeoutOffsetList = new ArrayList<>(remainTimeoutOffsetCache.asMap().keySet());
        return remainTimeoutOffsetList;
    }

    private List<OffsetRecord> buildContinuous(List<Offset> offsets) {
        // now: only one OffsetRecord;
        if (offsets.size() > 0) {
            return Arrays.asList(new OffsetRecord(offsets, offsets.get(0)));
        } else {
            return Lists.newArrayList();
        }
    }

    /**
     * This function does these things:
     * 1. find timeout Batch,
     * 2. then find all success offsets into remainSuccessOffsetCache,
     * 3. then all failed offsets
     * 4. then all timeout `.
     */
    private void cleanSelf() throws InterruptedException {
        long startTime = new Date().getTime();
        if (shouldDoClean()) { // still need to check weather should do clean.
            lastCleanTime = new Date().getTime(); // assign to new time, just in case cleanSelf() takes too much time.

            final long nowTime = new Date().getTime();

            Iterator<Map.Entry<Long, Batch>> iterator = batchTimestampMap.entries().iterator();

            // 可根据采用Timestamp的TreeMap来优化筛选过程，就不用挨个遍历
            while (iterator.hasNext()) {
                Map.Entry<Long, Batch> entry = iterator.next();
                Long timestamp = entry.getKey();

                if (nowTime - timestamp > TIMEOUT_THRESHOLD) {
                    Batch batch = entry.getValue();

                    successQueue.put(batch.popSuccessOffsets());
                    failQueue.put(batch.popFailOffsets());

//                        remainTimeoutOffsetCache.putAll(buildOffsetMap(batch.popTimeoutOffsets(), entry.getKey()));
                    List<Long> list = batch.popTimeoutOffsets();
                    remainTimeoutOffsetCache.putAll(buildOffsetMap(list, entry.getKey()));

                    iterator.remove();
                }
            }
            cleanTimer += new Date().getTime() - startTime;
            cleanCount++;
        }
    }

    private Map<Long, Long> buildOffsetMap(List<Long> offsetList, Long timestamp) {
        Map<Long, Long> result = new HashMap<>();
        for (Long offset : offsetList) {
            result.put(offset, timestamp);
        }
        return result;
    }


    private boolean shouldDoClean() {
        return lastCleanTime == -1 || (new Date().getTime() - lastCleanTime) > TIMEOUT_THRESHOLD;
    }

    private class Batch {
        EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf();
        EWAHCompressedBitmap successMap = EWAHCompressedBitmap.bitmapOf();
        EWAHCompressedBitmap failMap = EWAHCompressedBitmap.bitmapOf();
        long timestamp;

        public Batch(Iterator<Long> offsetList, long timestamp) {
            this.timestamp = timestamp;
            while (offsetList.hasNext()) {
                sendMap.set(offsetList.next().intValue());
            }
        }

        public void ackOffset(Long offset, Ack ack) {
            switch (ack) {
                case SUCCESS:
                    sendMap.clear(offset.intValue());
                    successMap.set(offset.intValue());
                    break;
                case FAIL:
                    sendMap.clear(offset.intValue());
                    failMap.set(offset.intValue());
                    break;
            }
        }

        public List<Long> popSuccessOffsets() {
            List<Long> result = new ArrayList<>();
            for (Integer index : successMap.toList()) {
                result.add((long) index);
            }

            successMap.clear();
            return result;
        }

        public List<Long> popFailOffsets() {
            List<Long> result = new ArrayList<>();
            for (Integer index : failMap.toList()) {
                result.add((long) index);
            }

            failMap.clear();
            return result;
        }

        public List<Long> popTimeoutOffsets() {
            // sendMap and ((not) ( successMap or failMap)) = remaining = timeoutMap
            EWAHCompressedBitmap timeoutMap = sendMap.andNot(successMap.or(failMap));
            List<Long> result = new ArrayList<>();

            for (Integer index : timeoutMap.toList()) {
                result.add((long) index);
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

        public boolean contains(long offset) {
            // 直接从sendMap取，而不是从offsetMap。可以避免发了一个，后续又重发导致offsetMap里面会有重复的情况，
            // 因为sendMap ack会清空，只有后一个Batch的sendMap有该值。
            return sendMap.get((int) offset);
        }

        /**
         * @return is all offsets of sendMap has been acked (whether success of fail)
         */
        public boolean isAllDone() {
            return sendMap.isEmpty();
        }
    }
}
