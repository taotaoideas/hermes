"use strict";
LocalDev.service("ProducerService", ['$http', '$q', function ($http, $q) {

    var msgs = [];
    var indexCounter = 1;

    function Message(index, timestamp, content, isOK, status) {
        this.index = index;
        this.timestamp = timestamp;
        this.content = content;
        this.isOK = isOK;
        this.status = status;
    }

    return {
        getProducerInfo: function () {
            return "Producer: Localhost-01"
        },

        getProducerMessageHistory: function () {
            return msgs;
        },

        sendMsg: function (topic, msg) {
            var date = new Date();
            msgs.push(new Message(indexCounter++, date.toLocaleString() + " (" + date.getTime() + ")", msg, "success", "OK"));

            $http.get("http://localhost:2765/api" + "/producer/send" + "?topic=" + topic + "&msg=" + msg)
        }
    }
}])