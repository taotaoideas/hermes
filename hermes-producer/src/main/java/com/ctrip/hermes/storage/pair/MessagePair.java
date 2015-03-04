package com.ctrip.hermes.storage.pair;

import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.spi.Storage;
import com.ctrip.hermes.storage.storage.Offset;

public class MessagePair extends AbstractPair<Record> {

    public MessagePair(Storage<Record> main, Storage<Offset> offset) {
        super(main, offset);
    }

}
