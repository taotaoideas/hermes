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

        //todo: refactor Consumer into global module.
        function Consumer(id, name, msgs) {
            this.id = id
            this.name = name
            this.msgs = msgs
        }

        function Message(index, timestamp, content, isOk, status) {
            this.index = index
            this.timestamp = timestamp
            this.content = content
            this.isOk = isOk
            this.status = status
        }


        var single_1 = new Consumer("s-1", "single-1",
            [new Message(1, 1011, "content111", "success", "ok")])

        var group1 = [], group2 = []

        var g1_1 = new Consumer("1-1", "group1-1",
            [new Message(1, 1011, "content111", "success", "ok")])
        var g1_2 = new Consumer("1-2", "group1-2",
            [new Message(1, 1011, "content111", "success", "ok")])
        group1.push(g1_1)
        group1.push(g1_2)


        var g2_1 = new Consumer("2-1", "group2-1",
            [new Message(1, 1011, "content111", "success", "ok")])
        var g2_2 = new Consumer("2-2", "group2-2",
            [new Message(1, 1011, "content111", "success", "ok")])
        var g2_3 = new Consumer("2-3", "group2-3",
            [new Message(1, 1011, "content111", "success", "ok")])
        group2.push(g2_1)
        group2.push(g2_2)
        group2.push(g2_3)


        var c = new Consumer("11", "consumer-c", "not ok");

        $scope.tabs = [
            {
                "title": "No Group",
                "content": [single_1]
            },
            {
                "title": "Group Name1",
                "content": group1
            },
            {
                "title": "Group Name2",
                "content": group2
            }
        ];
        $scope.tabs.activeTab = 0;
    })
    .controller("ConsumerCtrl", function ($scope) {


    })
