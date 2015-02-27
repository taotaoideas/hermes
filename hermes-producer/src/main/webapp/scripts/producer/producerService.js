"use strict";
LocalDev.service("ProducerService", ['$http', '$q', function($http, $q){
    return {
        getProducerInfo : function() {
            return "Producer: Localhost-01"
        },

        getProducerMessageHistory : function() {
            return [
                {"index": 1, "timestamp":"0011", "content":"test content111", "isOK": "success", "status": "OK." },
                {"index": 2, "timestamp":"0014", "content":"test content222", "isOK": "success", "status": "OK." },
                {"index": 3, "timestamp":"0015", "content":"test content333", "isOK": "danger", "status": "fail, need retry." },
                {"index": 4, "timestamp":"0021", "content":"test content444", "isOK": "danger", "status": "broker connection timeout." }
            ]
        }
    }
}])