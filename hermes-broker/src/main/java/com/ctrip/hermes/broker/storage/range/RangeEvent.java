package com.ctrip.hermes.broker.storage.range;

public class RangeEvent {

    private OffsetRecord m_record;

    public RangeEvent(OffsetRecord record) {
        m_record = record;
    }

    public OffsetRecord getRecord() {
        return m_record;
    }

}
