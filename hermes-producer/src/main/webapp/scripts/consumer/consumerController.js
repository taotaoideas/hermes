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
        $scope.tabs = [];

        setInterval(function() {
            updateTabs($scope.tabs);
        }, 1000)

        function updateTabs(tabs) {
            if (tabs.length == 0) {
                ConsumerService.getConsumerStatus($scope.topic).success(function(data, status, headers, config){
                    $scope.tabs=ConsumerService.handleData(data);
                    $scope.tabs.activeTab = 0;
                });
            } else {
                ConsumerService.getConsumerStatus($scope.topic).success(function(data, status, headers, config){
                    var d = ConsumerService.handleData(data)[$scope.tabs.activeTab];

                    $scope.tabs[$scope.tabs.activeTab].groupName = d.groupName;
                    $scope.tabs[$scope.tabs.activeTab].consumers = d.consumers;
                    $scope.tabs[$scope.tabs.activeTab].send = d.send;
                    $scope.tabs[$scope.tabs.activeTab].resend = d.resend;
                });
            }
        }
    })
    .controller("ConsumerCtrl", function ($scope) {

    })
