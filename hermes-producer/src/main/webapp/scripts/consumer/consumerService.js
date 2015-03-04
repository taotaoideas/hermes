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
        this.isOk = isOk;
        this.status = status;
    }

    var i = 1;
    return {
        getConsumerStatus: function (topic) {
            return $http.get("http://localhost:2765/api" + "/consumer/all");
        },
        handleData: function (data) {
            var result = [];

            for (var i = 0; i < data.length; i++) {
                var group = data[i];

                var consumers = []
                for (var j = 0; j < group.consumers.length; j++) {
                    var consumer = group.consumers[j];

                    var msgs = []
                    if (consumer.messages.length > 0) {
                        for (var k = 0; k < consumer.messages.length; k++) {
                            var msg = consumer.messages[k];
                            msgs.push(new Message(k, 0, msg.message, "success", "ok"))
                        }
                    }

                    consumers.push(new Consumer(consumer.name, msgs));
                }
                var mockConsumer = new Consumer("s-1", "single-1",
                    [new Message(1, 1011, "content111", "success", "ok")]);

                result.push(new ConsumerGroup(group.topic, group.groupName, consumers))
            }
            return result;
        }
    }
}])