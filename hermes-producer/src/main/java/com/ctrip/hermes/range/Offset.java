package com.ctrip.hermes.range;

public class Offset implements Comparable<Offset> {

    public final static long OLDEST = -1;

    private String m_id;
    private long m_offset;

    public Offset(String id, long offset) {
        m_id = id;
        m_offset = offset;
    }

    public long getOffset() {
        return m_offset;
    }

    public void setOffset(long offset) {
        m_offset = offset;
    }

    public String getId() {
        return m_id;
    }

    public void setId(String id) {
        m_id = id;
    }

    @Override
    public String toString() {
        return "Offset [m_id=" + m_id + ", m_offset=" + m_offset + "]";
    }

    @Override
    public int compareTo(Offset o) {
        return (int) (getOffset() - o.getOffset());
    }
}
