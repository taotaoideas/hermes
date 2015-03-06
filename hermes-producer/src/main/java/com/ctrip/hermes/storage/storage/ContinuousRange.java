package com.ctrip.hermes.storage.storage;

import java.util.List;

import com.ctrip.hermes.storage.util.CollectionUtil;

public class ContinuousRange implements Range {

    private String m_id;
    private Offset m_start;
    private Offset m_end;
    private Offset m_storageOffset;

    public ContinuousRange(Offset start) {
        this(start, start);
    }

    public ContinuousRange(Offset start, Offset end) {
        if (!start.getId().equals(end.getId())) {
            throw new RuntimeException("Offset with different id found in Range");
        }

        m_id = start.getId();
        m_start = start;
        m_end = end;
    }

    public ContinuousRange(List<Offset> offsets) {
        this(CollectionUtil.first(offsets), CollectionUtil.last(offsets));
    }

    public void setId(String id) {
   	 m_id = id;
    }
    
    @Override
    public String getId() {
        return m_id;
    }

    @Override
    public void setOffset(Offset offset) {
        m_storageOffset = offset;
    }

    @Override
    public Offset getOffset() {
        return m_storageOffset;
    }

    @Override
    public Offset getStartOffset() {
        return m_start;
    }

    @Override
    public Offset getEndOffset() {
        return m_end;
    }

    @Override
    public boolean contains(Offset o) {
        long offset = o.getOffset();
        return o.getId().equals(m_id) && //
                offset >= m_start.getOffset() && //
                offset <= m_end.getOffset();
    }

    @Override
    public String toString() {
        return "ContinuousRange [m_id=" + m_id + ", m_start=" + m_start + ", m_end=" + m_end + "]";
    }

	@Override
   public void setStartOffset(Offset offset) {
		m_start = offset;
   }

	@Override
   public void setEndOffset(Offset offset) {
		m_end = offset;
   }
    
    

}
