package com.ctrip.hermes.broker.storage.range;

import java.util.LinkedHashMap;
import java.util.List;

import com.ctrip.hermes.broker.storage.message.Ack;
import com.ctrip.hermes.broker.storage.storage.Offset;
import com.google.common.collect.Maps;
import com.googlecode.javaewah.EWAHCompressedBitmap;

public class OffsetBitmap {

    // 1. 由于EWAHCompressedBitmap是数字的，所以需要Map<int, Offset>的映射表，大多是insert和remove操作。
    LinkedHashMap<Long, Offset> offsetMap = Maps.newLinkedHashMap();
    EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf();
    EWAHCompressedBitmap successMap = EWAHCompressedBitmap.bitmapOf();
    EWAHCompressedBitmap fialMap = EWAHCompressedBitmap.bitmapOf();
    // 2. timeout使用Map<List<Offset>, long>的结构，每次遍历检查timeout的Offset

    // 3. 解决offset是long超过bitmap的int范围的问题
    public void putOffset(List<Offset> offsetList, long timestamp) {
        cleanSelf();

        for (Offset offset : offsetList) {
            offsetMap.put(offset.getOffset(), offset);
            sendMap.set((int)offset.getOffset());
        }
    }

    public void ackOffset(List<Offset> offsetList, Ack ack) {
        cleanSelf();

    }

    public OffsetRecord getAndRemoveSuccess() {

    }

    public OffsetRecord getAndRemoveFail() {

    }
    public OffsetRecord getAndRemoveTimeout() {
        cleanSelf();

    }

    /**
     * avoid OOM, should clean self: remove "old" data,
     * even if no put or ack or getAndRemove~ for long time.
     */
    private void cleanSelf() {

    }
}
