package com.ctrip.hermes.broker.storage.range;

import java.util.*;

import com.ctrip.hermes.broker.storage.message.Ack;
import com.ctrip.hermes.broker.storage.storage.ContinuousRange;
import com.ctrip.hermes.broker.storage.storage.Offset;
import com.ctrip.hermes.broker.storage.storage.Range;
import com.ctrip.hermes.broker.storage.storage.StorageException;
import com.googlecode.javaewah.EWAHCompressedBitmap;

public class MyDefaultRangeMonitor implements RangeMonitor {

    private List<RangeStatusListener> m_listeners = new ArrayList<RangeStatusListener>();
    private final long TIMEOUT_THRESHOLD = 3000;

    List<BatchStatus> batchStatuses = new ArrayList<>();
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
    public void startNewRange(OffsetRecord record) {

        Set<Offset> offsetBatch = new HashSet<>();
        EWAHCompressedBitmap bitmap = EWAHCompressedBitmap.bitmapOf();
        // TODO: 处理offset超过int最大值时的问题
        for (Offset offset : record.getToBeDone()) {
            offsetBatch.add(offset);
            bitmap.set((int) offset.getOffset());
        }

        batchStatuses.add(new BatchStatus(offsetBatch, bitmap, new Date().getTime()));
    }

    @Override
    public void offsetDone(OffsetRecord record, Ack ack) throws StorageException {
//        findAndUpdate(batchStatuses, offset);
//        if (!isAck) { //means failed
//            failOffsetSet.add(offset);
//        }

    }

//    @Override
//    public synchronized void startNewOffsets(Offset... offsets) {
//
//        Set<Offset> offsetBatch = new HashSet<>();
//        EWAHCompressedBitmap bitmap = EWAHCompressedBitmap.bitmapOf();
//        for (Offset offset : offsets) {
//            offsetBatch.add(offset);
//            bitmap.set((int) offset.getOffset());
//        }
//
//        batchStatuses.add(new BatchStatus(offsetBatch, bitmap, new Date().getTime()));
//    }

    public void offsetDone(Offset offset, boolean isAck) {
        // TODO: 处理offset超过int最大值时的问题
//        successMap.set((int) offset.getOffset());

    }

    private void findAndUpdate(List<BatchStatus> batchStatuses, Offset offset) {
        // TODO: improve the query performance
        for (BatchStatus batchStatus : batchStatuses) {
            Set<Offset> offsets = batchStatus.getOffsets();
            for (Offset o : offsets) {
                if (o.equals(offset)) {
                    batchStatus.setDoneMap((int) offset.getOffset());
                }
            }
        }
    }



    @Override
    public void addListener(RangeStatusListener listener) {
        m_listeners.add(listener);
    }

    private synchronized void notifyListener() {

        List<BatchStatus> doneBatchStatuses = removeDoneBatch(batchStatuses);
        List<BatchStatus> timeoutBatchStatuses = removeTimeoutBatch(batchStatuses, new Date().getTime(), TIMEOUT_THRESHOLD);

        List<Range> doneRanges = calculateDoneRanges(doneBatchStatuses, timeoutBatchStatuses);
        List<Range> timeoutRanges = calculateTimeoutRanges(timeoutBatchStatuses);
        List<Range> failRanges = calculateFailRange(failOffsetSet);

        // 3. notify listeners.
        for (RangeStatusListener listener : m_listeners) {
//            for (Range range : doneRanges) {
//                listener.onRangeSuccess(range);
//            }
//
//            for (Range range : failRanges) {
//                listener.onRangeFail(range);
//            }
        }
    }

    private List<Range> calculateFailRange(Set<Offset> failOffsetSet) {
        List<Range> rangeList = buildRangeListByOffsets(new TreeSet<>(failOffsetSet));
        return rangeList;
    }


    private List<Range> calculateTimeoutRanges(List<BatchStatus> timeoutBatchStatuses) {
        TreeSet<Offset> offsetSet = new TreeSet<>();
        for (BatchStatus batchStatus : timeoutBatchStatuses) {
            offsetSet.addAll(batchStatus.getOffsets());
        }

        List<Range> rangeList = buildRangeListByOffsets(offsetSet);
        return rangeList;
    }

    private List<Range> calculateDoneRanges(List<BatchStatus> doneBatchStatuses, List<BatchStatus> timeoutBatchStatuses) {
        TreeSet<Offset> offsetSet = new TreeSet<>();
        for (BatchStatus batchStatus : doneBatchStatuses) {
            offsetSet.addAll(batchStatus.getOffsets());
        }

        for (BatchStatus batchStatus : timeoutBatchStatuses) {
            Set<Offset> offsetInThisBatch = batchStatus.getOffsets();
            List<Integer> offsetList  = batchStatus.doneMap.toList();

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

    private List<BatchStatus> removeTimeoutBatch(List<BatchStatus> batchStatuses, long time, long timeoutThreshold) {
        List<BatchStatus> timeoutBatchStatuses = new ArrayList<>();
        Iterator<BatchStatus> iterator = batchStatuses.iterator();
        while (iterator.hasNext()) {
            BatchStatus batchStatus = iterator.next();
            if (batchStatus.isTimeout(time, timeoutThreshold)) {
                timeoutBatchStatuses.add(batchStatus);
                iterator.remove();
            }
        }
        return timeoutBatchStatuses;
    }

    private List<BatchStatus> removeDoneBatch(List<BatchStatus> batchStatuses) {
        List<BatchStatus> doneBatchStatuses = new ArrayList<>();
        Iterator<BatchStatus> iterator = batchStatuses.iterator();
        while (iterator.hasNext()) {
            BatchStatus batchStatus = iterator.next();
            if (batchStatus.isAllDone()) {
                doneBatchStatuses.add(batchStatus);
                iterator.remove();
            }
        }
        return doneBatchStatuses;
    }

    /*public EWAHCompressedBitmap calculateRanges(EWAHCompressedBitmap sendMap, EWAHCompressedBitmap successMap,
                                                EWAHCompressedBitmap failMap,
                                                List<Range> doneRanges, List<Range> failedRanges, List<Range> timeoutRanges, String
                                                        defaultOffsetId) {

        // 1.1 build DoneRanges:
        buildRangeByBitmap(successMap, doneRanges, defaultOffsetId);

        // 1.2 build FailRanges:
        buildRangeByBitmap(failMap, failedRanges, defaultOffsetId);

        // 2 get timeoutMap
        EWAHCompressedBitmap remainMap = EWAHCompressedBitmap.bitmapOf();
//        remainMap.setSizeInBits(sendMap.sizeInBits(), true);
        remainMap = sendMap.xor(successMap);

        EWAHCompressedBitmap timeoutMap = EWAHCompressedBitmap.bitmapOf();


        List<Integer> doneList = successMap.toList();
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

        successMap.clear();
        failMap.clear();
        timeoutMap.clear();

        return sendMap;
    }

    private void buildRangeByBitmap(EWAHCompressedBitmap successMap, List<Range> doneRanges, String defaultOffsetId) {
        int rangeStart = -1;
        int rangeEnd = -1;
        for (int done : successMap.toList()) {
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

    private class BatchStatus {
        Set<Offset> offsets;
        EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf();
        EWAHCompressedBitmap doneMap = EWAHCompressedBitmap.bitmapOf();
        long timestamp;

        public BatchStatus(Set<Offset> offsets, EWAHCompressedBitmap sendMap, long timestamp) {
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
