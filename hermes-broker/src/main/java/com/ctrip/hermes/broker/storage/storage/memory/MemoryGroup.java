package com.ctrip.hermes.broker.storage.storage.memory;

import java.util.ArrayList;
import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.storage.message.Message;
import com.ctrip.hermes.broker.storage.message.Resend;
import com.ctrip.hermes.broker.storage.pair.ClusteredMessagePair;
import com.ctrip.hermes.broker.storage.pair.MessagePair;
import com.ctrip.hermes.broker.storage.pair.ResendPair;
import com.ctrip.hermes.broker.storage.storage.Offset;

public class MemoryGroup {

    private MemoryStorageFactory m_storageFactory;
    private MemoryGroupConfig m_groupConfig;

    public MemoryGroup(MemoryStorageFactory storageFactory, MemoryGroupConfig groupConfig) {
        m_storageFactory = storageFactory;
        m_groupConfig = groupConfig;
    }

    public ClusteredMessagePair createMessagePair() {
        List<MessagePair> pairs = new ArrayList<MessagePair>();

        List<Pair<String, String>> mainIdPairs = m_groupConfig.mainGroupIds();

        for (Pair<String, String> idPair : mainIdPairs) {
            MemoryStorage<Message> m = m_storageFactory.findStorage(idPair.getKey());
            MemoryStorage<Offset> o = m_storageFactory.findStorage(idPair.getValue());
            pairs.add(new MessagePair(m, o));
        }

        return new ClusteredMessagePair(pairs);
    }

    public ResendPair createResendPair() {
        Pair<String, String> resendIdPair = m_groupConfig.resendGroupId();

        MemoryStorage<Resend> m = m_storageFactory.findStorage(resendIdPair.getKey());
        MemoryStorage<Offset> o = m_storageFactory.findStorage(resendIdPair.getValue());

        ResendPair pair = new ResendPair(m, o, Long.MAX_VALUE);

        return pair;
    }

}
