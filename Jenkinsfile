pipeline {
  agent any
  stages {
    stage('build') {
      steps {
        echo 'hello world'
      }
    }
    stage('error') {
      steps {
        perfReport(sourceDataFiles: 'aaa', configType: 'Xxvdfa', errorFailedThreshold: 11, errorUnstableResponseTimeThreshold: '1', ignoreFailedBuilds: true, modeOfThreshold: true, modePerformancePerTestCase: true, relativeFailedThresholdNegative: 2, relativeFailedThresholdPositive: -2, relativeUnstableThresholdNegative: 4)
      }
    }
  }
}