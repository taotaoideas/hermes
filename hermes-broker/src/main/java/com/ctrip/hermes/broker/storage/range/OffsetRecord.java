package com.ctrip.hermes.broker.storage.range;

import java.util.Arrays;
import java.util.List;

import com.ctrip.hermes.broker.storage.storage.Offset;

public class OffsetRecord {

    private List<Offset> m_toBeDone;
    private Offset m_toUpdate;

    public OffsetRecord(List<Offset> toBeDone, Offset toUpdate) {
        m_toBeDone = toBeDone;
        m_toUpdate = toUpdate;
    }

    public OffsetRecord(Offset toBeDone, Offset toUpdate) {
        m_toBeDone = Arrays.asList(toBeDone);
        m_toUpdate = toUpdate;
    }
    
    public OffsetRecord(Offset offset) {
        m_toBeDone = Arrays.asList(offset);
        m_toUpdate = offset;
    }

    public List<Offset> getToBeDone() {
        return m_toBeDone;
    }
    
    public Offset getToUpdate() {
        return m_toUpdate;
    }

    public boolean contains(OffsetRecord record) {
        return m_toBeDone.containsAll(record.getToBeDone());
    }

}
