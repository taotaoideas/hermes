package com.ctrip.hermes.broker.storage.range;

import java.util.*;

import com.ctrip.hermes.broker.storage.message.Ack;
import com.ctrip.hermes.broker.storage.storage.StorageException;

public class DefaultRangeMonitor implements RangeMonitor {

    private List<RangeStatusListener> m_listeners = new ArrayList<RangeStatusListener>();

    OldOffsetBitmap bitmap = new OldOffsetBitmap();

    public DefaultRangeMonitor() {
        Timer time = new Timer();
        time.schedule(new TimerTask() {
            @Override
            public void run() {
                notifyListener();
            }
        }, 1000, 1000);
    }

    @Override
    public void startNewRange(OffsetRecord record) {
        bitmap.putOffset(record.getToBeDone(), new Date().getTime());
    }

    @Override
    public void offsetDone(OffsetRecord record, Ack ack) throws StorageException {
        bitmap.ackOffset(record.getToBeDone(), ack);
    }

    @Override
    public void addListener(RangeStatusListener listener) {
        m_listeners.add(listener);
    }

    private void notifyListener() {
        // 1. get success RangeEvent
        List<OffsetRecord> success = bitmap.getAndRemoveSuccess();
        List<RangeEvent> successList = buildContinuousRange(success);

        // 2. get Fail RangeEvent
        List<OffsetRecord> fail = bitmap.getAndRemoveFail();
        List<RangeEvent> failList = buildContinuousRange(fail);

        // 3. get Timeout RangeEvent
        List<OffsetRecord> timeout = bitmap.getTimeoutAndRemove();
//        List<RangeEvent> timeoutList = buildContinuousRange(timeout);

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

    private List<RangeEvent> buildContinuousRange(List<OffsetRecord> recordList) {
        List<RangeEvent> eventList = new ArrayList<>();
        for (OffsetRecord offsetRecord : recordList) {
            eventList.add(new RangeEvent(offsetRecord));
        }
        return eventList;
    }
}
