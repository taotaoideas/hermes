"use strict";
LocalDev.service("MainService", ['$http', '$q', function ($http, $q) {
    var SERVICE_URL = "<server_url>/rest/slides";

    var selectedTopic = undefined;
    return {
        get: function () {
            return this.$http.get(this.LOAD_SLIDES_URL).error(this.errorCallback.bind(this));
        },
        post: function (data) {
            return this.$http.post(this.LOAD_SLIDES_URL, data).error(this.errorCallback.bind(this));
        },
        put: function (data) {
            return this.$http.put(this.LOAD_SLIDES_URL, data).error(this.errorCallback.bind(this));
        },
        errorCallback: function (response) { //Gracefully handle service error here. } } });
        },

        getTopicDropDown: function () {
           return $http.get("http://localhost:2765/api" + "/meta/topic");
        },
        handleTopicDropdown: function (data) {
            var dropdowns = [];

            for (var i = 0; i < data.length; i++) {
                var topic = data[i];
                var dropdown = {
                    "text": topic,
                    "click": "$setTopic(\"" + topic+ "\")"
                }
                dropdowns.push(dropdown)
            }
            return dropdowns;
        },

        setSelectedTopic: function(topic) {
           selectedTopic = topic;
        },

        getSelectedTopic : function() {
            return selectedTopic;
        }

    };
}]);
