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
        $scope.appid_dropdown = MainService.getAppIdDropdown();
        $scope.topic_dropdown = MainService.getTopicDropDown();

        $scope.$alert = function (title) {
            $alert({
                title: title,
                content: 'Best check yo self, you\'re not looking too good.',
                placement: 'top',
                type: 'info',
                keyboard: true,
                show: true
            });
        };

    })
