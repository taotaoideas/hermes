"use strict";
LocalDev
    .directive("consumer", function () {
        return {
            restrict: 'EA',
            replace: true,
            scope: {
                consumer: "=info"
            },
            templateUrl: 'views/consumer.html'
        }
    })
    .controller("ConsumerTabCtrl", function ($scope, $q, ConsumerService) {

        $scope.topic = "order.new";

        setTimeout(function(){
            ConsumerService.getConsumerStatus($scope.topic).success(function(data, status, headers, config){
                $scope.tabs = ConsumerService.handleData(data);
                $scope.tabs.activeTab = 0;
            });
        },1000);

        //$scope.tabs = [
        //    {
        //        "groupName": $scope.tabName,
        //        "consumers": [],
        //        "send" : {"nextOffset": 0},
        //        "resend": {"nextOffset": 0}
        //    },
        //    {
        //        "groupName": "Group Name1",
        //        "consumers": [],
        //        "send" : {"nextOffset": 1},
        //        "resend": {"nextOffset": 1}
        //    },
        //    {
        //        "groupName": "Group Name2",
        //        "consumers": [],
        //        "send" : {"nextOffset": 2},
        //        "resend": {"nextOffset": 2}
        //    }
        //];
        //$scope.tabs.activeTab = 0;

    })
    .controller("ConsumerCtrl", function ($scope) {


    })
