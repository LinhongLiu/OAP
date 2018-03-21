pipeline {
  agent any
  stages {
    stage('a') {
      steps {
        writeFile(file: 'a.csv', text: 'data1,data2\n1,3')
      }
    }
  }
}
