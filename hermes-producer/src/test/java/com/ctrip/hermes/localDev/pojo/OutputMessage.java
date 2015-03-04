package com.ctrip.hermes.localDev.pojo;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.message.MessagePackage;
import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.storage.Offset;

public class OutputMessage {

    public String message;
    public String key;
    public Offset offset;
    public Offset ackOffset;
    public Map<String, String> properties = new HashMap<String, String>();
    public long timestamp;

    public OutputMessage(String message, String key, Offset offset, Offset ackOffset, Map<String, String> properties,
                         long timestamp) {
        this.message = message;
        this.key = key;
        this.offset = offset;
        this.ackOffset = ackOffset;
        this.properties = properties;
        this.timestamp = timestamp;
    }

    public static OutputMessage convert(Message msg) {
        MessagePackage pkg = JSON.parseObject(((Message) msg).getContent(),
                MessagePackage.class);
        String body = null, key = null;
        if (null != pkg) {
            body = JSON.parseObject(pkg.getMessage(), String.class);
            key = JSON.parseObject(pkg.getKey(), String.class);
        }

        // todo: get timestamp from <Message>msg

        return new OutputMessage(body, key, msg.getOffset(), msg.getAckOffset(), msg.getProperties(), new Date().getTime());
    }
}