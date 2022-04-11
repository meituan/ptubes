#!/bin/bash
#usage
#   run default ptubes-sdk tasks
#       sh start.sh
case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac


PRE_DEPLOY_DIR=${bin_abs_path}/..
DEPLOY_DIR=`cd ${PRE_DEPLOY_DIR}; pwd`

PTUBES_CONF_FILE=${DEPLOY_DIR}/conf/sdk.conf
PTUBES_LOG_FILE=${DEPLOY_DIR}/log

echo "Task from config file '$PTUBES_CONF_FILE' is starting"
mkdir -p ${DEPLOY_DIR}
mkdir -p ${DEPLOY_DIR}/gclogs
mkdir -p ${DEPLOY_DIR}/log

LIBS_PATH="${DEPLOY_DIR}/lib"

classPath="${DEPLOY_DIR}:${DEPLOY_DIR}/conf/*"
for library in ${LIBS_PATH}/*.jar
do
  classPath="${classPath}:${library}"
done

JVM_MEM_OPTION=" -Xms1024m -Xmx1024m -XX:MaxDirectMemorySize=1024M"
JVM_GC_OPTIONS=" -XX:NewSize=8192m -XX:MaxNewSize=8192m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=85 -XX:+UseCMSInitiatingOccupancyOnly -XX:SurvivorRatio=14 -XX:MaxTenuringThreshold=15 "
JVM_GC_OPTIONS_2=" -XX:+CMSScavengeBeforeRemark -XX:+ScavengeBeforeFullGC -XX:+UseCMSCompactAtFullCollection -XX:+CMSParallelRemarkEnabled -XX:+ExplicitGCInvokesConcurrent "
JVM_GC_LOG_OPTIONS=" -XX:+PrintFlagsFinal -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M -Xloggc:${DEPLOY_DIR}/gclogs/gc.log "
JVM_HEAPDUMP="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${DEPLOY_DIR}/heapDump.hprof"
JVM_EXT="-Duser.dir=${DEPLOY_DIR} -Dptubes.sdk.log.dir=${PTUBES_LOG_FILE} -Dptubes.sdk.conf.dir=${PTUBES_CONF_FILE} -Dptubes.sdk.log.filename=default.log"

JVM_OPTIONS="${JVM_MEM_OPTION} ${JVM_GC_OPTIONS} ${JVM_GC_OPTIONS_2} ${JVM_GC_LOG_OPTIONS} ${JVM_HEAPDUMP} ${JVM_EXT}"

exec java -server -cp ${classPath} ${JVM_OPTIONS} -DisThreadContextMapInheritable=true -ea com.meituan.ptubes.sdk.example.ExampleByPropertiesFile > /dev/null 2>&1 &
echo "Task start request is send. Log file directory is $PTUBES_LOG_FILE"

