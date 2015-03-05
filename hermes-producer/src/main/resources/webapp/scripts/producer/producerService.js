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

        getProducerMessageHistory: function (topic) {
            return $http.get("http://localhost:2765/api" + "/producer/history" + "?topic=" + topic);
        },

        handleDate: function (data) {
            var msgs = [];
            for (var i = 0; i < data.length; i++) {
                var msg = data[i];
                var date = new Date(msg.timestamp);
                msgs.push(new Message(i, date.toLocaleString() /*+ " (" + date.getTime() + ")"*/,
                    msg.message, "success", "OK"));
            }
            return msgs;
        },

        sendMsg: function (topic, msg) {
            $http.get("http://localhost:2765/api" + "/producer/send" + "?topic=" + topic + "&msg=" + msg)
        }
    }
}])