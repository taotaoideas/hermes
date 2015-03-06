"use strict";
LocalDev.service("ConsumerService", ['$http', '$q', function ($http, $q) {


    function ConsumerGroup(topic, groupName, consumers) {
        this.topic = topic;
        this.groupName = groupName;
        this.consumers = consumers;
    }

    function Consumer(name, msgs) {
        this.name = name;
        this.msgs = msgs;
    }

    function Message(index, timestamp, content, isOk, status) {
        this.index = index;
        this.timestamp = timestamp;
        this.content = content;
        this.isOK = isOk;
        this.status = status;
    }

    function buildConsumerGroup(group) {
        var consumers = []
        for (var j = 0; j < group.consumers.length; j++) {
            var consumer = group.consumers[j];

            var msgs = []
            if (consumer.messages.length > 0) {
                for (var k = 0; k < consumer.messages.length; k++) {
                    var msg = consumer.messages[k];
                    var offset = msg.offset.offset;
                    var timestamp = new Date(msg.timestamp);
                    msgs.push(new Message(offset, timestamp.toLocaleString(), msg.message, "success", "OK"))
                }
            }

            consumers.push(new Consumer(consumer.name, msgs));
        }
        return new ConsumerGroup(group.topic, group.groupName, consumers);
    }

    var i = 1;
    return {
        getConsumerStatus: function (topic) {
            return $http.get("http://localhost:2765/api" + "/consumer" );
        },
        handleData: function (data) {
            var result = [];

            for (var i = 0; i < data.length; i++) {
                result.push(buildConsumerGroup(data[i]));
            }
            return result;
        }
    }
}])