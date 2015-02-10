package com.ctrip.hermes.broker.storage.range;

import java.util.*;

import com.ctrip.hermes.broker.storage.message.Ack;
import com.ctrip.hermes.broker.storage.storage.Offset;
import com.ctrip.hermes.broker.storage.storage.StorageException;

public class DefaultRangeMonitor implements RangeMonitor {

    private List<RangeStatusListener> m_listeners = new ArrayList<RangeStatusListener>();

    private TreeSet<RecordStatus> m_records = new TreeSet<>();

    OffsetBitmap bitmap = new OffsetBitmap();

    public DefaultRangeMonitor() {
        Timer time = new Timer();
        time.schedule(new TimerTask() {
            @Override
            public void run() {
                notifyListener();
            }
        }, 1000, 1000);
    }

    static class RecordStatus implements Comparable<RecordStatus> {
        private OffsetRecord m_record;
        private int m_doneCnt = 0;

        Set<Offset> successSet = new HashSet<>();
        Set<Offset> failSet = new HashSet<>();
        long timestamp;

        public void putSuccess(List<Offset> offsetList) {
            // not this simple!
            successSet.addAll(offsetList);
            m_doneCnt++;
        }

        public void putFail(List<Offset> offsetList) {
            // not this simple!
            failSet.addAll(offsetList);
            m_doneCnt++;
        }

        public boolean isTimeout(long comparedTime, long threshold) {
            return !((comparedTime - timestamp) <= threshold);
        }

        public RecordStatus(OffsetRecord record) {
            m_record = record;
        }

        public OffsetRecord getRecord() {
            return m_record;
        }

        @Override
        public int compareTo(RecordStatus o) {
            return (int) (this.timestamp - o.timestamp);
        }


        public boolean isDone() {
            return m_doneCnt == m_record.getToBeDone().size();
        }
    }

    @Override
    public void startNewRange(OffsetRecord record) {
        bitmap.putOffset(record.getToBeDone(), new Date().getTime());
//        m_records.add(new RecordStatus(record));
    }

    @Override
    public void offsetDone(OffsetRecord record, Ack ack) throws StorageException {
        bitmap.ackOffset(record.getToBeDone(), ack);
    }

    @Override
    public void addListener(RangeStatusListener lisener) {
        m_listeners.add(lisener);
    }

    private void notifyListener() {
        // 1. get success RangeEvent
        OffsetRecord success = bitmap.getAndRemoveSuccess();
        List<RangeEvent> successList = buildContinuousRange(success);

        // 2. get Fail RangeEvent
        OffsetRecord fail = bitmap.getAndRemoveFail();
        List<RangeEvent> failList = buildContinuousRange(fail);

        // 3. get Timeout RangeEvent
        OffsetRecord timeout = bitmap.getAndRemoveTimeout();
        List<RangeEvent> timeoutList = buildContinuousRange(timeout);

        // 4. notify listeners.
        for (RangeStatusListener listener : m_listeners) {
            for (RangeEvent event : successList) {
                listener.onRangeSuccess(event);
            }
            for (RangeEvent event : failList) {
                listener.onRangeFail(event);
            }
        }
    }

    private List<RangeEvent> buildContinuousRange(OffsetRecord record) {
//        List<Range> rangeList = new ArrayList<>();
//        Offset start = null;
//        Offset end = null;
//        for (Offset offset : record.getToBeDone()) {
//            if (null == start) {
//                start = offset;
//                end = offset;
//            } else {
//                if (offset.getOffset() == end.getOffset() + 1) {
//                    end = offset;
//                } else {
//                    Range tempRange = new ContinuousRange(start, end);
//                    rangeList.add(tempRange);
//                    start = null;
//                    end = null;
//
//                }
//            }
//        }
//        if (null != start && null != end) {
//            Range tempRange = new ContinuousRange(start, end);
//            rangeList.add(tempRange);
//        }
//        return rangeList;
        return null;
    }




/*    private List<Range> calculateFailRange(Set<Offset> failOffsetSet) {
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
            List<Integer> offsetList = batchStatus.doneMap.toList();

            for (Offset offset : offsetInThisBatch) {
                if (offsetList.contains((int) offset.getOffset())) {
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
    }*/
}
