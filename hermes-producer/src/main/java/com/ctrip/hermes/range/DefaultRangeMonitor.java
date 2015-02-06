package com.ctrip.hermes.range;

import java.util.*;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class DefaultRangeMonitor implements RangeMonitor {

    private List<RangeStatusListener> m_listeners = new ArrayList<RangeStatusListener>();

    EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf();
    EWAHCompressedBitmap doneMap = EWAHCompressedBitmap.bitmapOf();
    EWAHCompressedBitmap failMap = EWAHCompressedBitmap.bitmapOf();
    // 存放 offset.getOffset()--offset.getId();多为重复数据，可优化
    private Map<Integer, String> offsetIdMap = new HashMap<>();


    public DefaultRangeMonitor() {
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
        // TODO: 处理offset超过int最大值时的问题
        for (Offset offset : offsets) {
            sendMap.set((int) offset.getOffset());

            offsetIdMap.put((int) offset.getOffset(), offset.getId());
        }
    }

    @Override
    public void offsetDone(Offset offset, boolean isAck) {
        // TODO: 处理offset超过int最大值时的问题
        doneMap.set((int) offset.getOffset());

        if (!isAck) { //means failed
            failMap.set((int) offset.getOffset());
        }
    }

    @Override
    public void addListener(RangeStatusListener listener) {
        m_listeners.add(listener);
    }

    private synchronized void notifyListener() {
        System.out.println("notify start:" + new Date());
        // calculate
        List<Range> doneRanges = new ArrayList<>();
        List<Range> failedRanges = new ArrayList<>();
        List<Range> timeoutRanges = new ArrayList<>();

        sendMap = calculateRanges(sendMap, doneMap, failMap, doneRanges, failedRanges, timeoutRanges, null);

        // 3. notify listeners.
        for (RangeStatusListener listener : m_listeners) {
            System.out.println("doneRages: " + doneRanges.size() );
            for (Range range : doneRanges) {
                listener.onRangeDone(range);
            }
            System.out.println("failRanges: " + failedRanges.size());

            for (Range range : failedRanges) {
                listener.onRangeFail(range);
            }
        }
        System.out.println("notify end:" + new Date());

    }

    public EWAHCompressedBitmap calculateRanges(EWAHCompressedBitmap sendMap, EWAHCompressedBitmap doneMap,
                                                EWAHCompressedBitmap failMap,
                                                List<Range> doneRanges, List<Range> failedRanges, List<Range> timeoutRanges, String
                                                        defaultOffsetId) {

        // 1.1 build DoneRanges:
        buildRangeByBitmap(doneMap, doneRanges, defaultOffsetId);

        // 1.2 build FailRanges:
        buildRangeByBitmap(failMap, failedRanges, defaultOffsetId);

        // 2 get timeoutMap
        System.out.println("sendMap: " + sendMap.sizeInBits());
        EWAHCompressedBitmap remainMap = EWAHCompressedBitmap.bitmapOf();
//        remainMap.setSizeInBits(sendMap.sizeInBits(), true);
        remainMap = sendMap.xor(doneMap);

        System.out.println("remainMap: " + remainMap.sizeInBits());

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

    public static void main(String[] args) throws InterruptedException {
        DefaultRangeMonitor monitor = new DefaultRangeMonitor();
    }
}
