#!/usr/bin/env groovy

node('docker && linux-amd64') {
  stage('Checkout') {
    checkout scm
  }

  stage('Lint') {
    env.GOPATH = "${pwd tmp: true}/gopath"
    env.PATH="${env.GOPATH}/bin:${env.PATH}"
    sh "make lint"
  }

  stage('Build') {
    sh "make build"
  }

  stage('Test') {
    sh "make test"
  }
}
