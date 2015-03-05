package com.ctrip.hermes.local.pojo;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.message.StoredMessage;

public class MockConsumer {
    public String name;
    public List<OutputMessage> messages = new ArrayList<>();

    public MockConsumer(String name, List<OutputMessage> msgs) {
        this.name = name;
        this.messages = msgs;
    }

    public MockConsumer(String name) {
        this.name = name;
    }

    public void consumeOneMsg(StoredMessage<byte[]> msg) {
        messages.add(OutputMessage.convert(msg));
    }
}