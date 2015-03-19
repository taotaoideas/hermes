package com.ctrip.hermes.storage.range;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.unidal.tuple.Triple;

import com.ctrip.hermes.storage.storage.Offset;

public class BitmapTranslator {

    /**
     * 暂时先仅构造一个RangeEvent
     */
    public List<RangeEvent> buildContinuousRange(Triple<List<Long>, String, Offset> offsetList) {
        // todo: make RangeEvent continuous
        List<RangeEvent> eventList = new ArrayList<>();

        List<Offset> toBeDone = new ArrayList<>();
        for (Long offset : offsetList.getFirst()) {
            toBeDone.add(new Offset(offsetList.getMiddle(), offset));
        }

        eventList.add(new RangeEvent(new OffsetRecord(toBeDone, offsetList.getLast())));
        return eventList;
    }

    public void putOffset(NewOffsetBitmap bitmap, OffsetRecord record) {
        List<Long> offsetList = new ArrayList<>();

        if (record.getToBeDone().size() > 0) {
            String id = record.getToBeDone().get(0).getId();
            for (Offset offset : record.getToBeDone()) {
                offsetList.add(offset.getOffset());
            }

            bitmap.putOffset(offsetList.iterator(), id, record.getToUpdate(), new Date().getTime());
        }
    }

    public void ackOffset(NewOffsetBitmap bitmap, OffsetRecord record) {
        List<Long> offsets = offsetToLong(record.getToBeDone());
        bitmap.ackOffset(offsets.iterator(), record.getAck());
    }

    private List<Long> offsetToLong(List<Offset> list) {
        List<Long> result = new ArrayList<>();
        for (Offset offset : list) {
            result.add(offset.getOffset());
        }
        return result;
    }
}
