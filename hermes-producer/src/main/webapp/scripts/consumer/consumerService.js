"use strict";
LocalDev.service("ConsumerService", ['$http', '$q', function($http, $q){


    function ConsumerGroup(groupName, consumers, send, resend) {
        this.groupName = groupName;
        this.consumers = consumers;
        this.send = send;
        this.resend = send;
    }

    function Consumer(id, name, msgs) {
        this.id = id;
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

    return {
        getConsumerStatus : function(topic) {
            return $http.get("http://localhost:2765/api" + "/consumer"+"?topic="+topic);
        },
        handleData : function(data) {
            var result = [];

            for (var i = 0; i < data.length; i++) {
                var status = data[i];
                var mockConsumer =  new Consumer("s-1", "single-1",
                    [new Message(1, 1011, "content111", "success", "ok")]);

                result.push(new ConsumerGroup(status.group, [mockConsumer], status.sendNextOffset, status.resendNextOffset))
            }

            return result;
        }
    }
}])