#!/bin/bash

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

LOG_CONF_FILE=${DEPLOY_DIR}/conf/log4j2.xml
PTUBES_CONF_FILE=${DEPLOY_DIR}/conf/reader.conf

mkdir -p ${DEPLOY_DIR}
mkdir -p ${DEPLOY_DIR}/gclogs

LIBS_PATH="${DEPLOY_DIR}/lib"

classPath="${DEPLOY_DIR}:${DEPLOY_DIR}/conf/*"
for library in ${LIBS_PATH}/*.jar
do
  classPath="${classPath}:${library}"
done

JVM_MEM_OPTION=" -Xms4096m -Xmx4096m -XX:MaxDirectMemorySize=4096M"
JVM_GC_OPTIONS=" -XX:NewSize=2048m -XX:MaxNewSize=2048m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=85 -XX:+UseCMSInitiatingOccupancyOnly -XX:SurvivorRatio=14 -XX:MaxTenuringThreshold=15 "
JVM_GC_OPTIONS_2=" -XX:+CMSScavengeBeforeRemark -XX:+ScavengeBeforeFullGC -XX:+UseCMSCompactAtFullCollection -XX:+CMSParallelRemarkEnabled -XX:+ExplicitGCInvokesConcurrent "
JVM_GC_LOG_OPTIONS=" -XX:+PrintFlagsFinal -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M -Xloggc:${DEPLOY_DIR}/gclogs/gc.log "
JVM_HEAPDUMP="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${DEPLOY_DIR}/heapDump.hprof"
JVM_EXT="-Duser.dir=${DEPLOY_DIR} -Dptubes.conf=${PTUBES_CONF_FILE} -Dlog4j.configurationFile=${LOG_CONF_FILE}"

JVM_OPTIONS="${JVM_MEM_OPTION} ${JVM_GC_OPTIONS} ${JVM_GC_OPTIONS_2} ${JVM_GC_LOG_OPTIONS} ${JVM_HEAPDUMP} ${JVM_EXT}"

exec java -server -cp ${classPath} ${JVM_OPTIONS} -DisThreadContextMapInheritable=true -ea com.meituan.ptubes.reader.container.DemoMain > /dev/null 2>&1 &
