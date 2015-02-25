package com.ctrip.hermes.storage.range;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.storage.StorageException;

public class DefaultRangeMonitor implements RangeMonitor {

    private List<RangeStatusListener> m_listeners = new ArrayList<RangeStatusListener>();

    private List<RecordStatus> m_records = new ArrayList<RecordStatus>();

    static class RecordStatus {
        private OffsetRecord m_record;
        private int m_doneCnt = 0;

        public RecordStatus(OffsetRecord record) {
            m_record = record;
        }

        public OffsetRecord getRecord() {
            return m_record;
        }

        public boolean isDone() {
            return m_doneCnt == m_record.getToBeDone().size();
        }

        public void newDone() {
            m_doneCnt++;
        }

    }

    @Override
    public void startNewRange(OffsetRecord record) {
        m_records.add(new RecordStatus(record));
    }

    @Override
    public void offsetDone(OffsetRecord record, Ack ack) throws StorageException {
        // TODO merge to continuous range
        RecordStatus rs = findRecord(record);

        if (rs != null) {
            OffsetRecord owningRecord = rs.getRecord();
            switch (ack) {
            case SUCCESS:
                for (RangeStatusListener l : m_listeners) {
                    l.onRangeSuccess(new RangeEvent(owningRecord));
                }
                break;

            case FAIL:
                for (RangeStatusListener l : m_listeners) {
                    l.onRangeFail(new RangeEvent(owningRecord));
                }
                break;

            default:
                break;
            }
        } else {
            System.out.println("xx");
        }
    }

    private RecordStatus findRecord(OffsetRecord record) {
        Iterator<RecordStatus> iter = m_records.iterator();

        while (iter.hasNext()) {
            RecordStatus s = iter.next();
            if (s.getRecord().contains(record)) {
                // TODO
                s.newDone();
                if (s.isDone()) {
                    iter.remove();
                }
                return s;
            }
        }
        return null;
    }

    @Override
    public void addListener(RangeStatusListener lisener) {
        m_listeners.add(lisener);
    }

}
