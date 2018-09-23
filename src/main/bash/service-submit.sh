#! /bin/bash
# may need to remove quotes => $#
if ["$#" -ne 7]; then
    echo "[WARN] Usage: Calling this script requires 7 arguments i.e. <NODE> = the node the JAR will be stored, <USER> = the user the job will be executed by..."
    exit 1
fi

NODE="$1"
USER="$2"
APP_VERSION="$3"
HIVE_DATABASE="$4"
HIVE_FRAME_TABLE_NAME="$5"
STRATIFICATION_PROPERTIES_FILE_HDFS_FULL_PATH="$6"
SAMPLE_OUTPUT_HDFS_DIRECTORY="$7"

APP_DOMAIN=${8:-uk.gov.ons.sbr.service}
APP_MAIN=${9:-SamplingServiceMain}

#SPARK_PRINT_LAUNCH_COMMAND=1
#./bin/
spark2-submit \
    --verbose \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 6 \
    --driver-memory 4G \
    --executor-memory 30G \
    --class ${APP_DOMAIN}.${APP_MAIN} \
    hdfs://${NODE}/user/${USER}/lib/sbr-sampling-service/app/sbr-sampling-service-*.jar
