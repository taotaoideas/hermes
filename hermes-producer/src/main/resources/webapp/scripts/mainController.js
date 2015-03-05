"use strict";
var LocalDev = angular.module("Hermes", ['ngAnimate', 'ngSanitize', 'ngRoute', 'mgcrea.ngStrap'])

LocalDev
    // todo: handle server error before load the module.
    .config(["$httpProvider", function ($httpProvider) {
        var httpStatusCodeInterceptorFactory = function ($q) {
            function onSuccess(response) {
                if ("success_condition") {
                    return response.data;
                } else {
                    //Show your global error dialog
                    $q.reject(response.data);//Very important to reject the error
                }
            }

            function onError(response) { //Show your global error dialog
                $q.reject(response);//Very important to reject the error
            }

            return function (promise) {
                return promise.then(onSuccess, onError);
            }
        };
        //Activate your interceptor
        //$httpProvider.responseInterceptors.push(httpStatusCodeInterceptorFactory);
    }])
    .config(function ($dropdownProvider) {
        angular.extend($dropdownProvider.defaults, {
            html: true
        });
    })
    .controller("DropdownCtrl", function ($scope, $q, $alert, MainService) {

        updateDropdown();

        $scope.$setTopic = function(topic) {
            MainService.setSelectedTopic(topic);
        };

        $scope.$watch(function() {return MainService.getSelectedTopic()}, function() {
            $scope.selectedTopic = MainService.getSelectedTopic();
            updateDropdown();
        });

        function updateDropdown() {
            MainService.getTopicDropDown().success(function (data, status, headers, config) {
                $scope.topic_dropdown = MainService.handleTopicDropdown(data);

                if (MainService.getSelectedTopic() == undefined
                    && $scope.topic_dropdown.length > 0) {
                    $scope.$setTopic($scope.topic_dropdown[0].text);
                }
            });
        }
    })
    .controller("PopupCtrl", function ($scope, $q, $alert, MainService) {
        $scope.create = function(topic, group) {
            console.log("topic:" +topic + "; group: "+group);

            if (undefined != topic && undefined!=group) {
                MainService.addConsumer(topic, group);
            } else {
                alert("Must Input \"Topic\" and \"Group\"");
            }
        }
    });
