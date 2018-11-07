#!groovy
def server = Artifactory.server 'art-p-01'
def rtGradle = Artifactory.newGradleBuild()
rtGradle.tool = 'gradle_4.9'
rtGradle.resolver server: server, repo: 'VR_Regsiters'
rtGradle.deployer server: server, repo: 'registers-snapshots'
rtGradle.deployer.artifactDeploymentPatterns.addExclude("*.tar")
//rtGradle.deployer.artifactDeploymentPatterns.addExclude("*.${env.BUILD_ID}.zip")
def agentGradleVersion = 'gradle_4-9'
def distDir = 'build/dist/'

def buildInfo = Artifactory.newBuildInfo()


pipeline {
    libraries {
        lib('jenkins-pipeline-shared')
    }
    options {
        skipDefaultCheckout()
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '30'))
        timeout(time: 15, unit: 'MINUTES')
        ansiColor('xterm')
    }
    environment {
        ENVIRONMENT = "${params.ENV_NAME}"
        SUB_ORGANIZATION = "sbr"
        SERVICE_NAME = "$SUB_ORGANIZATION-sampling-service"
        STAGE = "PreBuild"

        EDGE_NODE = ""
        PROD_NODE = ""
    }
    parameters {
        choice(choices: 'dev\ntest\nbeta', description: 'Into what environment wants to deploy oozie config e.g. dev, test or beta?', name: 'ENV_NAME')
    }
    agent { label 'download.jenkins.slave' }
    stages {
        stage('Checkout') {
            agent { label 'download.jenkins.slave' }
            environment {
                STAGE = "Checkout"
            }
            steps {
                deleteDir()
                checkout scm
                script {
                    buildInfo.name = "${SERVICE_NAME}"
                    buildInfo.number = "${BUILD_NUMBER}"
                    buildInfo.env.collect()
                }
                stash name: 'Checkout'
            }
        }

        stage('Build') {
            agent { label "build.${agentGradleVersion}" }
            environment {
                STAGE = "Build"
            }
            steps {
                unstash name: 'Checkout'
                // TODO: fixup
                colourText("info", "Building ${env.BUILD_ID} on ${env.JENKINS_URL} from branch ${env.BRANCH_NAME}")
                sh "gradle clean compileScala"
                // milestone label: 'post build', ordinal: 1
            }
            post {
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }

//        stage('Validate') {
//            failFast true
//            parallel {
//                stage('Test: Unit') {
//                    agent { label "build.${agentGradleVersion}" }
//                    environment {
//                        STAGE = "Validate - Test: Unit"
//                    }
//                    steps {
//                        unstash name: 'Checkout'
//                        sh 'gradle test'
//                    }
//                    post {
//                        always {
//                            junit '**/build/test-results/test/*.xml'
//                            cobertura autoUpdateHealth: false,
//                                    autoUpdateStability: false,
//                                    coberturaReportFile: 'build/**/coverage-report/cobertura.xml',
//                                    conditionalCoverageTargets: '70, 0, 0',
//                                    failUnhealthy: false,
//                                    failUnstable: false,
//                                    lineCoverageTargets: '80, 0, 0',
//                                    maxNumberOfBuilds: 0,
//                                    methodCoverageTargets: '80, 0, 0',
//                                    onlyStable: false,
//                                    zoomCoverageChart: false
//                        }
//                        success {
//                            postSuccess()
//                        }
//                        failure {
//                            postFail()
//                        }
//                    }
//                }
//                stage('Style') {
//                    agent { label "build.${agentGradleVersion}" }
//                    environment {
//                        STAGE = "Validate - Test: Style"
//                    }
//                    steps {
//                        unstash name: 'Checkout'
//                        colourText("info", "Running style tests")
//                        sh 'gradle scalaStyle'
//                    }
//                    post {
//                        always {
//                            checkstyle canComputeNew: false, defaultEncoding: '', healthy: '', pattern: 'build/scala_style_result.xml', unHealthy: ''
//                        }
//                        success {
//                            postSuccess()
//                        }
//                        failure {
//                            postFail()
//                        }
//                    }
//                }
//            }
//            post {
//                success {
//                    postSuccess()
//                }
//                failure {
//                    postFail()
//                }
//            }
//        }

//        stage('Test: Acceptance') {
//            agent { label "build.${agentGradleVersion}" }
//            environment {
//                STAGE = 'Test: Acceptance'
//            }
//            steps {
//                unstash name: 'Checkout'
//                sh "gradle itest"
//            }
//            post {
//                always {
//                    junit '**/build/test-results/itest/*.xml'
//                }
//                success {
//                    postSuccess()
//                }
//                failure {
//                    postFail()
//                }
//            }
//        }

        stage('Publish') {
            agent { label "build.${agentGradleVersion}" }
            /*when {
                branch "master"
                // evaluate the when condition before entering this stage's agent, if any
                beforeAgent true
            }*/
            environment {
                STAGE = 'Publish'
            }
            steps {
                colourText("info", "Building ${env.BUILD_ID} on ${env.JENKINS_URL} from branch ${env.BRANCH_NAME}")
                unstash name: 'Checkout'
                script {
                    rtGradle.run tasks: 'clean artifactoryPublish'

                    sh "find . -name '*.jar' -type f"
                    sh "find . -name '*.zip' -type f"

                    def uploadSpec = """{
                        "files": [
                            {
                                "pattern": "build/libs/*.jar",
                                "target": "registers-snapshots/uk/gov/ons/sbr/${buildInfo.name}/${buildInfo.number}/"
                            },
                            { 
                                "pattern": "build/distributions/*.zip",
                                "target": "registers-snapshots/uk/gov/ons/sbr/${buildInfo.name}/${buildInfo.number}/"
                            }
                        ]
                    }"""
                    server.upload spec: uploadSpec, buildInfo: buildInfo
                }
            }
            post {
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }

        stage("Retrieve Artifact") {
            agent { label "deploy.jenkins.slave" }
            /*when {
                branch "master"
                // evaluate the when condition before entering this stage's agent, if any
                beforeAgent true
            }*/
            environment {
                STAGE = 'Retrieve Artifact'
            }
            steps {
                script {
                    def downloadSpec = """{
                        "files": [
                            {
                                "pattern": "registers-snapshots/uk/gov/ons/sbr/${buildInfo.name}/${buildInfo.number}/*",
                                "target": "${distDir}",
                                "flat": "true"
                            }
                        ]
                    }"""
                    server.download spec: downloadSpec, buildInfo: buildInfo
                }
                echo "archiving jar to [$ENVIRONMENT] environment"
                sh "ls -laR"
                sh "find . -name '*.jar' -type f"
                sshagent(["sbr-$ENVIRONMENT-ci-ssh-key"]) {
                    withCredentials([string(credentialsId: "prod_node", variable: 'PROD_NODE'), string(credentialsId: "edge_node", variable: 'EDGE_NODE')]) {
                        sh """
                            ssh -o StrictHostKeyChecking=no  sbr-$ENVIRONMENT-ci@$EDGE_NODE mkdir -p $SERVICE_NAME/
                            echo "Successfully created new directory [$SERVICE_NAME/]"
                            scp -r $distDir* sbr-$ENVIRONMENT-ci@$EDGE_NODE:$SERVICE_NAME/
                            echo "Successfully moved artifact [JAR] and scripts to $SERVICE_NAME/"
                            ssh -o StrictHostKeyChecking=no  sbr-$ENVIRONMENT-ci@$EDGE_NODE hdfs dfs -mkdir -p hdfs://$PROD_NODE/user/sbr-$ENVIRONMENT-ci/lib/$SERVICE_NAME/${buildInfo.number}
                            ssh -o StrictHostKeyChecking=no  sbr-$ENVIRONMENT-ci@$EDGE_NODE hdfs dfs -put -f $SERVICE_NAME/* hdfs://$PROD_NODE/user/sbr-$ENVIRONMENT-ci/lib/$SERVICE_NAME/${buildInfo.number}/
                            echo "Successfully copied jar file to HDFS"
                        """
                    }
                }

            }
            post {
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }
    }
    post {
        always {
            script {
                colourText("info", 'Post steps initiated')
                deleteDir()
            }
        }
        success {
            colourText("success", "All stages complete. Build was successful.")
            slackSend(
                    color: "good",
                    message: "${currentBuild.fullDisplayName} success: ${env.RUN_DISPLAY_URL}"
            )
        }
        unstable {
            colourText("warn", "Something went wrong, build finished with result ${currentBuild.result}. This may be caused by failed tests, code violation or in some cases unexpected interrupt.")
            slackSend(
                    color: "danger",
                    message: "${currentBuild.fullDisplayName} unstable: ${env.RUN_DISPLAY_URL}"
            )
        }
        failure {
            colourText("warn", "Process failed at: $STAGE")
            slackSend(
                    color: "danger",
                    message: "${currentBuild.fullDisplayName} failed at $STAGE: ${env.RUN_DISPLAY_URL}"
            )
        }
    }
}

def postSuccess() {
    colourText('info', "Stage: $STAGE successfull!")
}

def postFail() {
    colourText('warn', "Stage: $STAGE failed!")
}
