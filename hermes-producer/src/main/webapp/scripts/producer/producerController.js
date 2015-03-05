"use strict";
LocalDev.controller("ProducerCtrl", function ($scope, $q, MainService, ProducerService) {

    $scope.name = ProducerService.getProducerInfo();


    setInterval(function () {
        ProducerService.getProducerMessageHistory($scope.selectedTopic).success(function(data, status, headers, config){
            $scope.sent_msgs = ProducerService.handleDate(data);
        })
    }, 1000)

    $scope.tooltip = {
        //"title": "Tooltip: not null and invalid syntax validation!",
        message: undefined
    };

    $scope.alert = {
        "title": "Invalid Input!",
        "content": "Must input content!",
        "type": "info"
    };

    $scope.sendByEnter = function () {
        if (event.keyCode == 13) $scope.send();   // press <Enter> to send the message.
    }

    $scope.send = function () {
        if (undefined == $scope.tooltip.message) {

        } else {
            ProducerService.sendMsg($scope.selectedTopic, $scope.tooltip.message);
            $scope.tooltip.message = undefined;
        }
    }

    $scope.$watch(function() {return MainService.getSelectedTopic()}, function() {
        $scope.selectedTopic = MainService.getSelectedTopic();
    })
})