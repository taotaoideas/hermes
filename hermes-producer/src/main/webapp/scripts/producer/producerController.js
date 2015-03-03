"use strict";
LocalDev.controller("ProducerCtrl", function ($scope, $q, ProducerService) {

    $scope.name = ProducerService.getProducerInfo();

    $scope.sent_msgs = ProducerService.getProducerMessageHistory();

    $scope.tooltip = {
        "title": "Producer Input Tooltip...",
        message: undefined
    };

    $scope.sendByEnter = function() {
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