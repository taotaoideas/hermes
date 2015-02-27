"use strict";
LocalDev.service("QueueService", ['$http', '$q', function ($http, $q) {

    return {
        getQueueStatus: function () {
            return [
                {"index": 1, "timestamp": "0011", "content": "test content111", "isOK": "success", "status": "OK."},
                {"index": 2, "timestamp": "0014", "content": "test content222", "isOK": "success", "status": "OK."},
                {
                    "index": 3,
                    "timestamp": "0015",
                    "content": "test content333",
                    "isOK": "danger",
                    "status": "fail, need retry."
                },
                {
                    "index": 4,
                    "timestamp": "0021",
                    "content": "test content444",
                    "isOK": "danger",
                    "status": "broker connection timeout."
                }
            ]
        },

        getQueueInfo: function () {
            return "queue info"
        }
    }
}])