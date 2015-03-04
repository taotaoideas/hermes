"use strict";
LocalDev.controller("ProducerCtrl", function ($scope, $q, ProducerService) {

    $scope.topic = "order.new";

    $scope.name = ProducerService.getProducerInfo();


    setInterval(function () {
        ProducerService.getProducerMessageHistory($scope.topic).success(function(data, status, headers, config){
            $scope.sent_msgs = ProducerService.handleDate(data);
        })
    }, 1000)

    $scope.tooltip = {
        "title": "Tooltip: not null and invalid syntax validation!",
        message: undefined
    };

    $scope.sendByEnter = function () {
        if (event.keyCode == 13) $scope.send();
    }

    $scope.send = function () {
        if (undefined == $scope.tooltip.message) {
            alert("Must input Message(Todo: better alert)");
        } else {
            ProducerService.sendMsg("order.new", $scope.tooltip.message);
            $scope.tooltip.message = undefined;
        }
    }
})