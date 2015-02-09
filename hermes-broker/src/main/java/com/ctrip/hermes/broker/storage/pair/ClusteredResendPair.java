package com.ctrip.hermes.broker.storage.pair;

import java.util.List;

import com.ctrip.hermes.broker.storage.message.Resend;

public class ClusteredResendPair extends ClusteredPair<Resend> {

    private List<ResendPair> m_resendPairs;

    public ClusteredResendPair(List<ResendPair> childPairs) {
        super(childPairs);

        m_resendPairs = childPairs;
    }

    @Override
    protected int findPair(Resend resend) {
        for (int i = 0; i < m_resendPairs.size(); i++) {
            if (resend.getDue() - System.currentTimeMillis() <= m_resendPairs.get(i).getDueScale()) {
                return i;
            }
        }

        return m_resendPairs.size() - 1;
    }

}
