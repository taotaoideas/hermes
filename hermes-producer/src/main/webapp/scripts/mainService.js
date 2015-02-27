"use strict";
LocalDev.service("MainService", ['$http', '$q', function ($http, $q) {
    var SERVICE_URL = "<server_url>/rest/slides";
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


        getAppIdDropdown : function () {
            return  [
                {
                    "text": "<i class=\"fa fa-download\"></i>&nbsp;All AppId",
                    "href": "#anotherAction"
                },
                {
                    "text": "<i class=\"fa fa-globe\"></i>&nbsp;340101",
                    "click": "$alert(\"Holy guacamole!\")"
                },
                {
                    "text": "<i class=\"fa fa-external-link\"></i>&nbsp;920101",
                    "href": "/auth/facebook",
                    "target": "_self"
                },
                {
                    "divider": true
                },
                {
                    "text": "Separated link",
                    "href": "#separatedLink"
                }
            ];
        },

        getTopicDropDown : function() {
            return [
                {
                    "text": "<i class=\"fa fa-download\"></i>&nbsp;All Topic",
                    "href": "#anotherAction"
                },
                {
                    "text": "<i class=\"fa fa-globe\"></i>&nbsp;order.new",
                    "click": "$alert(\"Holy guacamole!\")"
                },
                {
                    "text": "<i class=\"fa fa-external-link\"></i>&nbsp;order.confirm",
                    "href": "/auth/facebook",
                    "target": "_self"
                },
                {
                    "divider": true
                },
                {
                    "text": "Separated link",
                    "href": "#separatedLink"
                }
            ];
        }

    };
}]);
