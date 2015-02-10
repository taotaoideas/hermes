package com.ctrip.hermes.range;

import java.util.*;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class MyDefaultRangeMonitor implements RangeMonitor {

    private List<RangeStatusListener> m_listeners = new ArrayList<RangeStatusListener>();
    private final long TIMEOUT_THRESHOLD = 3000;

    List<Batch> batches = new ArrayList<>();
    Set<Offset> failOffsetSet = new HashSet<>();

    public MyDefaultRangeMonitor() {
        Timer time = new Timer();
        time.schedule(new TimerTask() {
            @Override
            public void run() {
                notifyListener();
            }
        }, 2000, 2000);
    }

    @Override
    public synchronized void startNewOffsets(Offset... offsets) {

        Set<Offset> offsetBatch = new HashSet<>();
        EWAHCompressedBitmap bitmap = EWAHCompressedBitmap.bitmapOf();
        // TODO: 处理offset超过int最大值时的问题
        for (Offset offset : offsets) {
            offsetBatch.add(offset);
            bitmap.set((int) offset.getOffset());
        }

        batches.add(new Batch(offsetBatch, bitmap, new Date().getTime()));
    }

    @Override
    public void offsetDone(Offset offset, boolean isAck) {
        // TODO: 处理offset超过int最大值时的问题
//        doneMap.set((int) offset.getOffset());

        findAndUpdate(batches, offset);
        if (!isAck) { //means failed
            failOffsetSet.add(offset);
        }
    }

    private void findAndUpdate(List<Batch> batches, Offset offset) {
        // TODO: improve the query performance
        for (Batch batch : batches) {
            Set<Offset> offsets = batch.getOffsets();
            for (Offset o : offsets) {
                if (o.equals(offset)) {
                    batch.setDoneMap((int) offset.getOffset());
                }
            }
        }
    }

    @Override
    public void addListener(RangeStatusListener listener) {
        m_listeners.add(listener);
    }

    private synchronized void notifyListener() {

        List<Batch> doneBatches = removeDoneBatch(batches);
        List<Batch> timeoutBatches = removeTimeoutBatch(batches, new Date().getTime(), TIMEOUT_THRESHOLD);

        List<Range> doneRanges = calculateDoneRanges(doneBatches, timeoutBatches);
        List<Range> timeoutRanges = calculateTimeoutRanges(timeoutBatches);
        List<Range> failRanges = calculateFailRange(failOffsetSet);

        // 3. notify listeners.
        for (RangeStatusListener listener : m_listeners) {
            for (Range range : doneRanges) {
                listener.onRangeDone(range);
            }

            for (Range range : failRanges) {
                listener.onRangeFail(range);
            }
        }
    }

    private List<Range> calculateFailRange(Set<Offset> failOffsetSet) {
        List<Range> rangeList = buildRangeListByOffsets(new TreeSet<>(failOffsetSet));
        return rangeList;
    }


    private List<Range> calculateTimeoutRanges(List<Batch> timeoutBatches) {
        TreeSet<Offset> offsetSet = new TreeSet<>();
        for (Batch batch : timeoutBatches) {
            offsetSet.addAll(batch.getOffsets());
        }

        List<Range> rangeList = buildRangeListByOffsets(offsetSet);
        return rangeList;
    }

    private List<Range> calculateDoneRanges(List<Batch> doneBatches, List<Batch> timeoutBatches) {
        TreeSet<Offset> offsetSet = new TreeSet<>();
        for (Batch batch : doneBatches) {
            offsetSet.addAll(batch.getOffsets());
        }

        for (Batch batch : timeoutBatches) {
            Set<Offset> offsetInThisBatch = batch.getOffsets();
            List<Integer> offsetList  = batch.doneMap.toList();

            for (Offset offset : offsetInThisBatch) {
                if (offsetList.contains((int)offset.getOffset())) {
                    offsetSet.add(offset);
                }
            }
        }

        List<Range> rangeList = buildRangeListByOffsets(offsetSet);
        return rangeList;
    }

    private List<Range> buildRangeListByOffsets(TreeSet<Offset> offsetSet) {
        List<Range> rangeList = new ArrayList<>();
        Offset start = null;
        Offset end = null;
        for (Offset offset : offsetSet) {
            if (null == start) {
                start = offset;
                end = offset;
            } else {
                if (offset.getOffset() == end.getOffset() + 1) {
                    end = offset;
                } else {
                    Range tempRange = new ContinuousRange(start, end);
                    rangeList.add(tempRange);
                    start = null;
                    end = null;

                }
            }
        }
        if (null != start && null != end) {
            Range tempRange = new ContinuousRange(start, end);
            rangeList.add(tempRange);
        }
        return rangeList;
    }

    private List<Batch> removeTimeoutBatch(List<Batch> batches, long time, long timeoutThreshold) {
        List<Batch> timeoutBatches = new ArrayList<>();
        Iterator<Batch> iterator = batches.iterator();
        while (iterator.hasNext()) {
            Batch batch = iterator.next();
            if (batch.isTimeout(time, timeoutThreshold)) {
                timeoutBatches.add(batch);
                iterator.remove();
            }
        }
        return timeoutBatches;
    }

    private List<Batch> removeDoneBatch(List<Batch> batches) {
        List<Batch> doneBatches = new ArrayList<>();
        Iterator<Batch> iterator = batches.iterator();
        while (iterator.hasNext()) {
            Batch batch = iterator.next();
            if (batch.isAllDone()) {
                doneBatches.add(batch);
                iterator.remove();
            }
        }
        return doneBatches;
    }

    /*public EWAHCompressedBitmap calculateRanges(EWAHCompressedBitmap sendMap, EWAHCompressedBitmap doneMap,
                                                EWAHCompressedBitmap failMap,
                                                List<Range> doneRanges, List<Range> failedRanges, List<Range> timeoutRanges, String
                                                        defaultOffsetId) {

        // 1.1 build DoneRanges:
        buildRangeByBitmap(doneMap, doneRanges, defaultOffsetId);

        // 1.2 build FailRanges:
        buildRangeByBitmap(failMap, failedRanges, defaultOffsetId);

        // 2 get timeoutMap
        EWAHCompressedBitmap remainMap = EWAHCompressedBitmap.bitmapOf();
//        remainMap.setSizeInBits(sendMap.sizeInBits(), true);
        remainMap = sendMap.xor(doneMap);

        EWAHCompressedBitmap timeoutMap = EWAHCompressedBitmap.bitmapOf();


        List<Integer> doneList = doneMap.toList();
        int maxDoneOffset = -1;
        if (doneList.size() > 0) {
            maxDoneOffset = doneList.get(doneList.size() - 1);
        }

        for (int remain : remainMap.toList()) {
            if (remain <= maxDoneOffset) {
                timeoutMap.set(remain);
            } else {
                break;
            }
        }

        // 3. sendMap = remain - timeout
        sendMap = remainMap.xor(timeoutMap);

        buildRangeByBitmap(timeoutMap, timeoutRanges, defaultOffsetId);

        doneMap.clear();
        failMap.clear();
        timeoutMap.clear();

        return sendMap;
    }

    private void buildRangeByBitmap(EWAHCompressedBitmap doneMap, List<Range> doneRanges, String defaultOffsetId) {
        int rangeStart = -1;
        int rangeEnd = -1;
        for (int done : doneMap.toList()) {
            if (rangeStart == -1) {
                rangeStart = done;
            }
            if (rangeEnd == -1) {
                rangeEnd = done;
            } else if (done == rangeEnd + 1) {
                rangeEnd = done;
            } else {
                addNewRange(doneRanges, rangeStart, rangeEnd, defaultOffsetId);

                rangeStart = done;
                rangeEnd = done;
            }
        }
        if (-1 != rangeStart && -1 != rangeEnd) {
            addNewRange(doneRanges, rangeStart, rangeEnd, defaultOffsetId);
        }
    }

    private void addNewRange(List<Range> doneRanges, int rangeStart, int rangeEnd, String defaultOffsetId) {
        // if not exist in offsetIdMap, id is null
        Offset start, end;
        if (null == defaultOffsetId) {
            start = new Offset(offsetIdMap.get(rangeStart), (long) rangeStart);
            end = new Offset(offsetIdMap.get(rangeEnd), (long) rangeEnd);
        } else {
            start = new Offset(defaultOffsetId, (long) rangeStart);
            end = new Offset(defaultOffsetId, (long) rangeEnd);
        }
        doneRanges.add(new ContinuousRange(start, end));
    }
*/

    private class Batch {
        Set<Offset> offsets;
        EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf();
        EWAHCompressedBitmap doneMap = EWAHCompressedBitmap.bitmapOf();
        long timestamp;

        public Batch(Set<Offset> offsets, EWAHCompressedBitmap sendMap, long timestamp) {
            this.offsets = offsets;
            this.sendMap = sendMap;
            this.timestamp = timestamp;
        }

        public Set<Offset> getOffsets() {
            return offsets;
        }

        public void setSendMap(int position) {
            this.sendMap.set(position);
        }

        public void clearSendMap(int position) {
            this.sendMap.clear(position);
        }

        public void setDoneMap(int position) {
            this.doneMap.set(position);
        }

        public void clearDoneMap(int position) {
            this.doneMap.clear(position);
        }

        public boolean isTimeout(long comparedTime, long threshold) {
            return !((comparedTime - timestamp) <= threshold);
        }

        public boolean isAllDone() {
            return sendMap.equals(doneMap);
        }
    }

}
