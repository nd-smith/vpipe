pipeline {
    agent any

    environment {
        // Project settings
        PROJECT_NAME = 'kafka-pipeline'
        IMAGE_NAME = 'kafka-pipeline:latest'

        // Environment selection (override via Jenkins parameter)
        DEPLOY_ENV = "${params.ENVIRONMENT ?: 'production'}"
        ENV_FILE = ".env.${DEPLOY_ENV}"

        // Docker compose settings
        COMPOSE_FILE = 'docker-compose.yml'
        COMPOSE_PROJECT = 'vpipe'
    }

    parameters {
        choice(
            name: 'ENVIRONMENT',
            choices: ['production', 'eventhub', 'local'],
            description: 'Deployment environment'
        )
        booleanParam(
            name: 'RUN_TESTS',
            defaultValue: true,
            description: 'Run test suite before deployment'
        )
        booleanParam(
            name: 'ENABLE_KAFKA',
            defaultValue: false,
            description: 'Start local Kafka (for testing only)'
        )
        string(
            name: 'SCALE_DOWNLOAD',
            defaultValue: '3',
            description: 'Number of download worker instances'
        )
        string(
            name: 'SCALE_UPLOAD',
            defaultValue: '3',
            description: 'Number of upload worker instances'
        )
    }

    stages {
        stage('Checkout') {
            steps {
                echo "Checking out code for ${DEPLOY_ENV} environment"
                checkout scm

                // Verify environment file exists
                script {
                    if (!fileExists(ENV_FILE)) {
                        error "Environment file ${ENV_FILE} not found. Available: .env.local, .env.eventhub, .env.production"
                    }
                }
            }
        }

        stage('Environment Setup') {
            steps {
                echo 'Setting up Python environment'
                sh '''
                    python3 --version
                    pip3 --version
                '''

                // Copy environment-specific .env file
                sh """
                    cp ${ENV_FILE} .env
                    echo "Using environment configuration: ${ENV_FILE}"
                """
            }
        }

        stage('Install Dependencies') {
            when {
                expression { params.RUN_TESTS == true }
            }
            steps {
                echo 'Installing Python dependencies'
                sh '''
                    cd src
                    pip3 install -r requirements.txt
                '''
            }
        }

        stage('Run Tests') {
            when {
                expression { params.RUN_TESTS == true }
            }
            steps {
                echo 'Running test suite'
                sh '''
                    cd src
                    pytest tests/ --cov=. --cov-report=xml --cov-report=html
                '''
            }
            post {
                always {
                    // Publish test results
                    junit '**/test-results/*.xml'

                    // Publish coverage report
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: 'htmlcov',
                        reportFiles: 'index.html',
                        reportName: 'Coverage Report'
                    ])
                }
            }
        }

        stage('Build Docker Image') {
            steps {
                echo "Building Docker image: ${IMAGE_NAME}"
                sh """
                    docker build -t ${IMAGE_NAME} .
                """

                // Verify image was created
                sh """
                    docker images | grep ${PROJECT_NAME}
                """
            }
        }

        stage('Stop Old Containers') {
            steps {
                echo 'Stopping existing containers'
                script {
                    // Stop and remove old containers gracefully
                    sh """
                        docker-compose -p ${COMPOSE_PROJECT} -f ${COMPOSE_FILE} down || true
                    """
                }
            }
        }

        stage('Deploy') {
            steps {
                echo "Deploying to ${DEPLOY_ENV} environment"
                script {
                    def kafkaProfile = params.ENABLE_KAFKA ? '--profile kafka' : ''
                    def scaleArgs = "--scale xact-download=${params.SCALE_DOWNLOAD} --scale xact-upload=${params.SCALE_UPLOAD} --scale claimx-downloader=${params.SCALE_DOWNLOAD} --scale claimx-uploader=${params.SCALE_UPLOAD}"

                    sh """
                        docker-compose -p ${COMPOSE_PROJECT} -f ${COMPOSE_FILE} ${kafkaProfile} up -d ${scaleArgs}
                    """
                }
            }
        }

        stage('Health Check') {
            steps {
                echo 'Verifying deployment'
                script {
                    // Wait for containers to start
                    sleep(time: 10, unit: 'SECONDS')

                    // Check container status
                    sh """
                        docker-compose -p ${COMPOSE_PROJECT} ps
                    """

                    // Verify workers are running
                    def runningContainers = sh(
                        script: "docker-compose -p ${COMPOSE_PROJECT} ps --services --filter 'status=running' | wc -l",
                        returnStdout: true
                    ).trim()

                    echo "Running containers: ${runningContainers}"

                    if (runningContainers.toInteger() < 5) {
                        error "Not enough containers running. Expected at least 5, got ${runningContainers}"
                    }
                }
            }
        }

        stage('Cleanup') {
            steps {
                echo 'Cleaning up old Docker images'
                sh '''
                    # Remove dangling images
                    docker image prune -f

                    # Remove old kafka-pipeline images (keep latest)
                    docker images | grep kafka-pipeline | grep -v latest | awk '{print $3}' | xargs -r docker rmi || true
                '''
            }
        }
    }

    post {
        success {
            echo 'Pipeline completed successfully!'

            // Show container logs for verification
            sh """
                echo "=== Container Status ==="
                docker-compose -p ${COMPOSE_PROJECT} ps

                echo "=== Recent Logs ==="
                docker-compose -p ${COMPOSE_PROJECT} logs --tail=20
            """

            // Notify success (configure your notification method)
            // slackSend(color: 'good', message: "Deployment successful: ${env.JOB_NAME} #${env.BUILD_NUMBER}")
        }

        failure {
            echo 'Pipeline failed!'

            // Capture logs for debugging
            sh """
                docker-compose -p ${COMPOSE_PROJECT} logs --tail=100 > deployment-failure-logs.txt
            """

            // Archive failure logs
            archiveArtifacts artifacts: 'deployment-failure-logs.txt', allowEmptyArchive: true

            // Notify failure (configure your notification method)
            // slackSend(color: 'danger', message: "Deployment failed: ${env.JOB_NAME} #${env.BUILD_NUMBER}")
        }

        always {
            // Clean workspace
            cleanWs()
        }
    }
}
