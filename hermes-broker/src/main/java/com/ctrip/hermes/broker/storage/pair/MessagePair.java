package com.ctrip.hermes.broker.storage.pair;

import com.ctrip.hermes.broker.storage.message.Message;
import com.ctrip.hermes.broker.storage.spi.Storage;
import com.ctrip.hermes.broker.storage.storage.Offset;

public class MessagePair extends AbstractPair<Message> {

    public MessagePair(Storage<Message> main, Storage<Offset> offset) {
        super(main, offset);
    }

}
