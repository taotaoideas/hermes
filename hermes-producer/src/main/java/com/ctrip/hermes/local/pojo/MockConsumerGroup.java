package com.ctrip.hermes.local.pojo;

import java.util.List;

public class MockConsumerGroup {
    public String topic;
    public String groupName;
    public List<MockConsumer> consumers;

    public MockConsumerGroup(String topic, String groupName, List<MockConsumer> consumers) {
        this.topic = topic;
        this.groupName = groupName;
        this.consumers = consumers;
    }
}