"use strict";
LocalDev.controller("QueueCtrl", function($scope, $q, QueueService){

    $scope.msgs = QueueService.getQueueStatus();

});
