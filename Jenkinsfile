pipeline {
  agent any
  stages {
    stage('a') {
      steps {
        writeFile(file: 'a.csv', text: '''data1,data2
1,3''')
      }
    }
    stage('') {
      steps {
        perfReport 'a.csv'
      }
    }
  }
}