package com.ctrip.hermes.storage.range;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ctrip.hermes.storage.storage.Offset;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class BitmapTranslator {

    Logger log = LogManager.getLogger(NewOffsetBitmap.class);

    final int CACHE_SIZE = 50 * 1000;

    public Cache<Long /*offset*/, Offset /*Offset Object*/> offsetMap = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE).expireAfterWrite(5, TimeUnit.MINUTES).
                    removalListener(new RemovalListener<Long, Offset>() {
                        @Override
                        public void onRemoval(RemovalNotification<Long, Offset> note) {
                            if (offsetMap.size() == CACHE_SIZE) {
                                log.error(String.format("Cache offsetMap is full! size is %s, remove offset:%s, %s."
                                        ,offsetMap.size(), note.getKey(), note.getKey()));
                            } else {
                                log.info(String.format("Cache remove offset:%s, %s.", note.getKey(), note.getKey()));
                            }
                        }
                    }).build();

    public List<RangeEvent> buildContinuousRange(List<Long> offsetList) {
        // todo: make RangeEvent continuous
        List<RangeEvent> eventList = new ArrayList<>();
        eventList.add(new RangeEvent(new OffsetRecord(longToOffset(offsetList), null)));
        return eventList;
    }

    public void putOffset(NewOffsetBitmap bitmap, OffsetRecord record) {
        List<Long> offsetList = new ArrayList<>();
        for (Offset offset : record.getToBeDone()) {
            offsetMap.put(offset.getOffset(), offset);
            offsetList.add(offset.getOffset());
        }

        bitmap.putOffset(offsetList, new Date().getTime());
    }

    public void ackOffset(NewOffsetBitmap bitmap, OffsetRecord record) {
        List<Long> offsets = offsetToLong(record.getToBeDone());
        bitmap.ackOffset(offsets, record.getAck());
    }

    private List<Long> offsetToLong(List<Offset> list) {
        List<Long> result = new ArrayList<>();
        for (Offset offset : list) {
            result.add(offset.getOffset());
        }
        return result;
    }

    private List<Offset> longToOffset(List<Long> list) {
        List<Offset> result = new ArrayList<>();
        List<Long> notfound = new ArrayList<>();
        for (Long offset : list) {
            if (offsetMap.getIfPresent(offset) != null) {
                result.add(offsetMap.getIfPresent(offset));
            } else {
                notfound.add(offset);
            }
        }

        if (notfound.size() > 0) {
            log.error("Not Found Offsets: " + notfound.toString());
        }
        return result;
    }
}
