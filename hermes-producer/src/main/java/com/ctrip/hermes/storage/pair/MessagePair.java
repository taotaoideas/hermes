package com.ctrip.hermes.storage.pair;

import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.spi.Storage;
import com.ctrip.hermes.storage.storage.Offset;

public class MessagePair extends AbstractPair<Message> {

    public MessagePair(Storage<Message> main, Storage<Offset> offset) {
        super(main, offset);
    }

}
