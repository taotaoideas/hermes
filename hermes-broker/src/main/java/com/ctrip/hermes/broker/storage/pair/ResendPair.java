package com.ctrip.hermes.broker.storage.pair;

import com.ctrip.hermes.broker.storage.message.Resend;
import com.ctrip.hermes.broker.storage.spi.Storage;
import com.ctrip.hermes.broker.storage.storage.Offset;

public class ResendPair extends AbstractPair<Resend> {

    private long m_dueScale;

    public ResendPair(Storage<Resend> main, Storage<Offset> offset, long dueScale) {
        super(main, offset);

        m_dueScale = dueScale;
    }

    public long getDueScale() {
        return m_dueScale;
    }

}
