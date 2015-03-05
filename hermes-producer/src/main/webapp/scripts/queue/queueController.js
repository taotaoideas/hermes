"use strict";
LocalDev.controller("QueueCtrl", function($scope, $q, QueueService, MainService){

    $scope.$watch(function() {return MainService.getSelectedTopic()}, function() {
        $scope.topic = MainService.getSelectedTopic();
    })

    setInterval(function(){
        QueueService.getQueueStatus($scope.topic).success(function(data, status, headers, config){
            $scope.msgs = QueueService.handleData(data);
        });
    },1000);
});
