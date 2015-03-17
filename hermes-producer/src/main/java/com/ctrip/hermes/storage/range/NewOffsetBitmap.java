package com.ctrip.hermes.storage.range;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.storage.Offset;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.IntIterator;

/**
 * 每次batch将多个offset存放于各独立的bitmap中。
 */
public class NewOffsetBitmap {

    Logger log = LogManager.getLogger(NewOffsetBitmap.class);


    ExecutorService service = Executors.newSingleThreadExecutor();
    LinkedBlockingQueue<Triple<List<Long>, String, Offset>> allQueue;
    LinkedBlockingQueue<Triple<List<Long>, String, Offset>> failQueue;

    public NewOffsetBitmap(LinkedBlockingQueue<Triple<List<Long>, String, Offset>> allQueue,
                           LinkedBlockingQueue<Triple<List<Long>, String, Offset>> failQueue) {
        this.allQueue = allQueue;
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
    Cache<Long /*offset*/, Long /*timestamp*/> remainTimeoutOffsetCache = CacheBuilder.newBuilder()
            .maximumSize(20 * 1000).expireAfterWrite(10, TimeUnit.MINUTES).build();

    long putTimer = 0, ackTimer = 0, cleanTimer = 0;
    int putCount = 0, ackCount = 0, cleanCount = 0;

    private Runnable putOffsetRunnable(final Iterator<Long> offsetList, final String id,
                                       final Offset toUpdate, final long timestamp) {
        return new Runnable() {
            @Override
            public void run() {
                long startTime = new Date().getTime();
                batchTimestampMap.put(timestamp, new Batch(offsetList, id, toUpdate, timestamp));

                putTimer += new Date().getTime() - startTime;
                putCount++;
            }
        };
    }

    private Runnable ackOffsetRunnable(final Iterator<Long> offsetList, final Ack ack) {
        return new Runnable() {
            @Override
            public void run() {
                long startTime = new Date().getTime();

                while(offsetList.hasNext()) {
                    Long offset = offsetList.next();
                    Batch batch = findBatch(batchTimestampMap, offset);
                    if (null != batch) {
                        batch.ackOffset(offset, ack);
                        // put into queue, if all batch is done (weather success or fail)
                        if (batch.isAllDone()) {
                            try {
                                List<Long> all = batch.popAll();
                                if (all.size() > 0) {
                                    allQueue.put(Triple.from(all, batch.getId(), batch.getToUpdate()));
                                }
                                batch.setBeenTaken(true);
                                List<Long> fail = batch.popFailOffsets();
                                if (fail.size() > 0 ) {
                                    failQueue.put(Triple.from(fail, batch.getId(), batch.getToUpdate()));
                                }
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

    public void putOffset(Iterator<Long> offsetList, String id, Offset toUpdate, long timestamp) {
        try {
            putCleanTask();
            service.submit(putOffsetRunnable(offsetList, id, toUpdate, timestamp));
        } catch (InterruptedException e) {
            log.info("NewOffsetBitmap: putOffset() is Interrupted!");
        }
    }

    public void ackOffset(Iterator<Long> offsetList, Ack ack) {
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

    public List<Long> getTimeoutAndRemove() {
        try {
            putCleanTask();
        } catch (InterruptedException e) {
            log.info("NewOffsetBitmap: getTimeoutAndRemove() is Interrupted!");
        }

        List<Long> remainTimeoutOffsetList = new ArrayList<>(remainTimeoutOffsetCache.asMap().keySet());
        return remainTimeoutOffsetList;
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

                    if (!batch.isBeenTaken()) {  // means it haven't put into allQueue yet
                        List<Long> all = batch.popAll();
                        if (all.size() > 0 ) {
                            allQueue.put(Triple.from(all, batch.getId(), batch.getToUpdate()));
                        }

                        List<Long> failList = batch.popFailOffsets();
                        // Here, regard Timeout as Fail:
                        failList.addAll(batch.popTimeoutOffsets());

                        if (failList.size() > 0) {
                            failQueue.put(Triple.from(failList, batch.getId(), batch.getToUpdate()));
                        }

                        List<Long> list = batch.popTimeoutOffsets();
                        remainTimeoutOffsetCache.putAll(buildOffsetMap(list, entry.getKey()));

                    }
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

        public boolean isBeenTaken() {
            return isBeenTaken;
        }

        public void setBeenTaken(boolean isBeenTaken) {
            this.isBeenTaken = isBeenTaken;
        }

        boolean isBeenTaken = false;
        public String getId() {
            return id;
        }

        public Offset getToUpdate() {
            return toUpdate;
        }

        String id;
        Offset toUpdate;

        public Batch(Iterator<Long> offsetList, String id, Offset toUpdate, long timestamp) {
            this.timestamp = timestamp;
            this.id = id;
            this.toUpdate = toUpdate;
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

        /**
         * pop all offsets including success, fail, and timeout.
         */
        public List<Long> popAll() {
            List<Long> result = new ArrayList<>();
            EWAHCompressedBitmap allMap = sendMap.or(successMap.or(failMap));
            for (Integer index : allMap.toList()) {
                result.add((long) index);
            }

            return result;
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
            // sendMap and ((not) ( successMap or failMap)) = remaining == timeoutMap
            EWAHCompressedBitmap timeoutMap = sendMap.andNot(successMap.or(failMap));
            List<Long> result = new ArrayList<>();

            for (Integer index : timeoutMap.toList()) {
                result.add((long) index);
            }
            // here is no need to clear related offsets in sendMap, as this node will be removed soon.
            return result;
        }

        public boolean contains(long offset) {
            // 直接从sendMap取，可以避免发了一个，后续又重发导致offsetMap里面会有重复的情况，
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

    public static void main(String[] args) {
        EWAHCompressedBitmap b = EWAHCompressedBitmap.bitmapOf();

        b.set(1000);
        b.set(100000);

        IntIterator it = b.reverseIntIterator();


        while(it.hasNext())  {
            System.out.println(it.next());
        }
    }
}
