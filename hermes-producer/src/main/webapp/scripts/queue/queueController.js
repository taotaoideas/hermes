"use strict";
LocalDev.controller("QueueCtrl", function($scope, $q, QueueService){

    $scope.topic = "order.new";

    setInterval(function(){
        QueueService.getQueueStatus($scope.topic).success(function(data, status, headers, config){
            $scope.msgs = QueueService.handleData(data);
        });
    },1000);
});
