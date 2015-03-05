package com.ctrip.hermes.local.pojo;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.storage.Offset;

public class OutputMessage {

    public String message;

    public String key;

    public Offset offset;

    public Offset ackOffset;

    public Map<String, Object> properties = new HashMap<String, Object>();

    public long timestamp;

    public OutputMessage(String message, String key, Offset offset, Offset ackOffset, Map<String, Object> properties,
                         long timestamp) {
        this.message = message;
        this.key = key;
        this.offset = offset;
        this.ackOffset = ackOffset;
        this.properties = properties;
        this.timestamp = timestamp;
    }

    public static OutputMessage convert(Record msg) {
        String body = null, key = null;
        body = new String(msg.getContent());
        key = msg.getKey();

        // todo: get timestamp from <Record>msg
        long timestamp;
        if (msg.getBornTime() == null) {
            timestamp = new Date().getTime();
        } else {
            timestamp = msg.getBornTime();
        }

        return new OutputMessage(body, key, msg.getOffset(), msg.getAckOffset(), msg.getProperties(),
                timestamp);
    }

    public static OutputMessage convert(StoredMessage<byte[]> msg) {
        String body = null, key = null;
        body = new String(msg.getBody());
        key = msg.getKey();

        // todo: get timestamp from <Message>msg

        return new OutputMessage(body, key, msg.getOffset(), msg.getAckOffset(), new HashMap<String, Object>(),
                new Date().getTime());
    }
}