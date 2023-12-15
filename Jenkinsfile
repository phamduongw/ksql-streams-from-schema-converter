pipeline {
    agent any

    tools {
        dockerTool "myDocker"
    }

    environment {
        DOCKER_IMAGE_NAME = "ksql-streams-from-schema-converter-api"
        DOCKER_IMAGE_TAG = "${readJSON(file: 'package.json').version}"
    }

    stages {
        stage("Build") {
            steps {
                sh "docker build -t ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG} ."
            }
        }

        stage("Stop and Remove"){
            steps {
                sh "docker stop ${DOCKER_IMAGE_NAME} || echo 'This container does not exist.'"
                sh 'echo y | docker container prune'
            }
        }

        stage("Deploy") {
            steps {
                sh "docker run -d -p 8080:80 --name ${DOCKER_IMAGE_NAME} ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG}"
            }
        }
    }
}