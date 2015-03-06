"use strict";
LocalDev.service("QueueService", ['$http', '$q', function ($http, $q) {

    function Status(index, timestamp, content, isOK, status) {
        this.index = index
        this.timestamp = timestamp;
        this.content = content;
        this.isOK = isOK;
        this.status = status;
    }

    return {
        getQueueStatus: function (topic) {
            return $http.get("http://localhost:2765/api" + "/queue"+"?topic="+topic);
        },

        handleData: function (data) {
            var result = []
            var topicInfo = data;
            for (var i = 0; i < topicInfo.length; i++) {
                var timestamp = new Date(topicInfo[i].timestamp);
                result.push(new Status(i + 1, timestamp.toLocaleString(), topicInfo[i].message, "success", "OK"));
            }
            return result;
        }
    }
}])