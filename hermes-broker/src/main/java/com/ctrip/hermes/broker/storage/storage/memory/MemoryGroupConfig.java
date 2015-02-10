package com.ctrip.hermes.broker.storage.storage.memory;

import java.util.ArrayList;
import java.util.List;

import org.unidal.tuple.Pair;

public class MemoryGroupConfig {

    private List<Pair<String, String>> m_mainGroupIds = new ArrayList<Pair<String, String>>();
    private Pair<String, String> m_resendGroupId;

    public void addMainGroup(String mainId, String offsetId) {
        m_mainGroupIds.add(new Pair<String, String>(mainId, offsetId));
    }

    public void setResendGroupId(String mainId, String offsetId) {
        m_resendGroupId = new Pair<String, String>(mainId, offsetId);
    }

    public List<Pair<String, String>> mainGroupIds() {
        return m_mainGroupIds;
    }

    public Pair<String, String> resendGroupId() {
        return m_resendGroupId;
    }

}
