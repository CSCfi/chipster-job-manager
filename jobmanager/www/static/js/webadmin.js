angular.module('WebAdmin', ['ui.bootstrap', 'ngRoute']);

angular.module('WebAdmin').controller('NavCtrl', function($scope, $route, $routeParams, $location) {
        $scope.items = [
            {path: '/', title: 'Dashboard'},
            {path: '/jobs', title: 'Jobs'},
            {path: '/maintenance', title: 'Maintenance'},
        ];
        $scope.isActive = function(item) {
            if (item.path == $location.path()) {
                return 'active';
            }
            return '';
        };
        $scope.$route = $route;
        $scope.$location = $location;
        $scope.$routeParams = $routeParams;
    })
    .controller('DashboardCtrl', function($scope, $http) {
        $http.get('/jobs/?active=True').
            success(function(data, status, headers, config) {
                $scope.jobs = data;
            }).
            error(function(data, status, headers, config) {
                alert("data loading error");
            });
    })
    .controller('JobsCtrl', function($scope, $http) {
         $http.get('/jobs').
            success(function(data, status, headers, config) {
                $scope.jobs = data;
            }).
            error(function(data, status, headers, config) {
                alert("data loading error");
            });       
    })
    .controller('JobResultModalCtrl', function($scope, $modal, $http) {
        $scope.open = function(job_id) {
            $http.get('/jobs/'+job_id+'/results').success(function(data) {
                $scope.results = data;
                var modalInstance = $modal.open({
                    templateUrl: 'jobresultcontent.html',
                    controller: 'JobResultModalInstanceCtrl',
                    resolve: {
                        results: function () {
                            return $scope.results;
                        }   
                    }
                });
            });
        };
    })
    .controller('JobResultModalInstanceCtrl', function($scope, $modalInstance, results) {
        $scope.results = results;
        $scope.ok = function() {
            $modalInstance.close();      
        };

        $scope.cancel = function() {
            $modalInstance.dismiss('cancel');
        };
    })
    .controller('MaintenanceCtrl', function($scope, $http) {
        
    })
    .config(['$routeProvider', function($routeProvider) {
        $routeProvider
        .when('/', {
            templateUrl: 'static/views/dashboard.html',
            controller: 'DashboardCtrl'
        })
        .when('/jobs', {
            templateUrl: 'static/views/jobs.html',
            controller: 'JobsCtrl'
        })
        .when('/maintenance', {
            templateUrl: 'static/views/maintenance.html',
            controller: 'MaintenanceCtrl'
        })
        .otherwise({
            redirectTo: '/'
        });
    }])
    .directive('holderFix', function() {
        return {
            link: function(scope, element, attrs) {
                Holder.run({ images: element[0], nocss: true});
            }
        };
    });


