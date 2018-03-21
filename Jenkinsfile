pipeline {
  agent any
  stages {
    stage('a') {
      steps {
        writeFile(file: 'a.csv', text: 'data1,data2\n1,3')
      }
    }
    stage('') {
      steps {
        plot csvFileName: 'plot-232eb9a5-28b9-4442-b11d-ed0f2f61549e.csv', exclZero: false, group: 'a', keepRecords: false, logarithmic: false, numBuilds: '10', style: 'line', title: 'a', useDescr: false, yaxis: 'a', yaxisMaximum: '1', yaxisMinimum: '1'
      }
    }
  }
}
