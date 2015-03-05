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
    .controller("ConsumerTabCtrl", function ($scope, $q, ConsumerService, MainService) {
        $scope.tabs = [];

        setInterval(function() {
            $scope.topic = MainService.getSelectedTopic();
            //console.log($scope.topic)
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
                    if ($scope.tabs.activeTab == -1 ||$scope.tabs.activeTab > data.length) {
                        $scope.tabs.activeTab = 0
                    }
                    var d = ConsumerService.handleData(data)[$scope.tabs.activeTab];

                    $scope.tabs[$scope.tabs.activeTab].groupName = d.groupName;
                    $scope.tabs[$scope.tabs.activeTab].consumers = d.consumers;
                    $scope.tabs[$scope.tabs.activeTab].send = d.send;
                    $scope.tabs[$scope.tabs.activeTab].resend = d.resend;
                });
            }
        }
        $scope.checkTopic =function (tab) {
            return tab.topic == $scope.topic;
        }
    })
    .controller("ConsumerCtrl", function ($scope) {

    })
