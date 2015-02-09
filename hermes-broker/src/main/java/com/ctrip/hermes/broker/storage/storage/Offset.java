package com.ctrip.hermes.broker.storage.storage;

public class Offset {

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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_id == null) ? 0 : m_id.hashCode());
        result = prime * result + (int) (m_offset ^ (m_offset >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Offset other = (Offset) obj;
        if (m_id == null) {
            if (other.m_id != null)
                return false;
        } else if (!m_id.equals(other.m_id))
            return false;
        if (m_offset != other.m_offset)
            return false;
        return true;
    }

}
