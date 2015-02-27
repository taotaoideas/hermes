"use strict";
LocalDev.controller("ProducerCtrl", function($scope, $q, ProducerService){

    $scope.name = ProducerService.getProducerInfo();

    $scope.sent_msgs = ProducerService.getProducerMessageHistory();

    $scope.tooltip = {
        "title": "Producer Input Tooltip..."
    }
})