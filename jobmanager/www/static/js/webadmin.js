var app = angular.module("WebAdmin", []);

app.controller("WebAdminCtrl", function($scope, $http) {
    $http.get('/jobs/').
        success(function(data, status, headers, config) {
            $scope.jobs = data;
            console.log(data);
        }).
        error(function(data, status, headers, config) {
            alert("data loading error");
        });
});
