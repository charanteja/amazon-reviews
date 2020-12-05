// This Jenkinsfile uses the declarative syntax. If you need help, check:
// Overview and structure: https://jenkins.io/doc/book/pipeline/syntax/
// For plugins and steps:  https://jenkins.io/doc/pipeline/steps/
// For Github integration: https://github.com/jenkinsci/pipeline-github-plugin
// Docker:                 https://jenkins.io/doc/book/pipeline/docker/
// Environment variables:  env.VARIABLE_NAME

pipeline {
    agent any

    stages {
        stage('Clone') {
            steps {
                checkout scm
            }
        }
        stage('Compile') {
            when {
                changeRequest()
            }
            steps {
                sh 'sbt compile'
            }
        }
        stage('Test') {
            when {
                changeRequest()
            }
            steps {
                sh 'sbt test'
            }
        }
        stage('Package') {
            when {
                anyOf {
                    branch 'master'; // Only package jar on master
                    branch 'sandbox'; // ... sandbox
                    changeRequest() // ... and PRs
                }
            }
            steps {
                sh 'sbt assembly'
            }
            post {
                always {
                    archiveArtifacts artifacts: '*/target/*.jar', fingerprint: true
                }
            }
        }
        stage('Docker') {
            when {
                anyOf {
                    branch 'master'; // Only build docker images on master
                    branch 'sandbox'; // ... sandbox
                    changeRequest() // ... and PRs
                }
            }
            steps {
                script {
                    if (env.CHANGE_ID && pullRequest.comments[-1]?.body != 'docker push') {
                        return
                    }
                    def name = "eu.gcr.io/amazon-reviews:${env.GIT_COMMIT}"
                    docker.build(name).push()
                    if (env.CHANGE_ID) { //PR
                        pullRequest.comment("pushed `$name`")
                    }
                }
            }
        }
    }
}

