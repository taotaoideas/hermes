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
    .controller("ConsumerTabCtrl", function ($scope, $q, $window, ConsumerService, MainService) {
        $scope.tabs = [];

        setInterval(function () {
            $scope.topic = MainService.getSelectedTopic();
            updateTabs($scope.tabs);
        }, 1000)

        function updateTabs(tabs) {
            if (tabs.length == 0) {
                ConsumerService.getConsumerStatus($scope.topic).success(function (data, status, headers, config) {
                    $scope.tabs = ConsumerService.handleData(data);
                    $scope.tabs.activeTab = 0;
                });
            } else {
                ConsumerService.getConsumerStatus($scope.topic).success(function (data, status, headers, config) {
                    var dataResult = ConsumerService.handleData(data);

                    for (var i = 0; i < $scope.tabs.length; i++) {
                        $scope.tabs[i].groupName = dataResult[i].groupName;

                        $scope.tabs[i].consumers = dataResult[i].consumers;
                    }

                    for (var i = $scope.tabs.length; i < dataResult.length; i++) {
                        $scope.tabs.push(dataResult[i]);
                    }
                });
            }
        }

        $scope.isShowTab = function (tab) {
            return tab.topic == $scope.topic;
        };

        $scope.scroll = function () {
            //alert("scroll");
            //setTimeout(function () {
            //    $window.scrollBy(100, document.body.scrollHeight - 500);
            //}, 300);
        }
    })
    .controller("ConsumerCtrl", function ($scope) {

    })
