#!/bin/bash

SCRIPTNAME=$(basename $0)
ENVINFOFILE="environment_info.txt"

while [ $# -gt 0 ]; do
	case "$1" in
	-node_basedir)
		if [ "$2" = "" ]; then
			echo "$SCRIPTNAME: Value missing for -node_basedir"
		fi
		NODE_BASEDIR=$2
		shift
		shift
		;;
	-branch_name)
		if [ "$2" = "" ]; then
			echo "$SCRIPTNAME: Value missing for -branch_name"
		fi
		BRANCH_NAME=$2
		shift
		shift
		;;
	-java_home)
		if [ "$2" = "" ]; then
			echo "$SCRIPTNAME: Value missing for -java_home"
		fi
		export JAVA_HOME=$2
		shift
		shift
		;;
	-mvn_home)
		if [ "$2" = "" ]; then
			echo "$SCRIPTNAME: Value missing for -mvn_home"
		fi
		export MVN_HOME=$2
		shift
		shift
		;;
	-task_name)
		if [ "$2" = "" ]; then
			echo "$SCRIPTNAME: Value missing for -task_name"
		fi
		TASK_NAME=$2
		shift
		shift
		;;
	-jenkins_host)
		if [ "$2" = "" ]; then
			echo "$SCRIPTNAME: Value missing for -jenkins_host"
		fi
		JENKINS_HOST=$2
		shift
		shift
		;;
	-jenkins_user)
		if [ "$2" = "" ]; then
			echo "$SCRIPTNAME: Value missing for -jenkins_user"
		fi
		JENKINS_USER=$2
		shift
		shift
		;;
	-test_hosts)
		if [ "$2" = "" ]; then
			echo "$SCRIPTNAME: Value missing for -test_hosts"
		fi
		TEST_HOSTS=$2
		shift
		shift
		;;
	-test_args)
		if [ "$2" = "" ]; then
			echo "$SCRIPTNAME: Value missing for -test_args"
		fi
		TEST_ARGS=$2
		shift
		shift
		;;
	*)
		echo "$SCRIPTNAME: Unknown option: $1"
		exit 1
		;;
	esac
done

if [ "$TASK_NAME" = "" ]; then
	TASK_NAME="current test"
fi

#if [ "$TASK_NAME" = "kv_unit_erasure" ]; then
#    TEST_ARGS="-Dtest.je.eraser.props=je.env.runEraser=true;\je.erase.period=1\ s;"
#fi

if [ "$BRANCH_NAME" = "" ]; then
	BRANCH_NAME="main"
fi

if [ "$NODE_BASEDIR" = "" ]; then
	NODE_BASEDIR="/scratch/tests"
fi

if [ "$JAVA_HOME" == "" ]; then
	echo "No environment variable JAVA_HOME. Environment JAVA_HOME must be set."
	exit 1
fi

if [ "$MVN_HOME" == "" ]; then
	echo "No environment variable MVN_HOME. Environment MVN_HOME must be set."
	exit 1
fi

export PATH=$JAVA_HOME/bin:$MVN_HOME/bin:$PATH

echo "Task ${TASK_NAME} is running..."

#ROOT_DIR=`cd ../../../../../ && pwd`
ROOT_DIR=${NODE_BASEDIR}/workspace/${TASK_NAME}
LOG_DIR=${ROOT_DIR}/../../log_archive
KV_DIR=${ROOT_DIR}/kv

cd ${KV_DIR} && git checkout ${BRANCH_NAME}

KV_REV_ID=$(git describe --abbrev=12 --always --dirty=+)

echo ""
echo "=================================================="
echo ""
echo "Revision id: ${KV_REV_ID}"
cd ${ROOT_DIR}/kv && git show ${KV_REV_ID} >./jenkins_changeset.txt

JAVA_VERN=$(java -version 2>&1)
MVN_VERSION=$(mvn -version)
KV_VERSION=$(cd ${KV_DIR} && git log -n 1 ${KV_REV_ID})

echo "Java: ${JAVA_VERN}"
echo "Maven: $MVN_VERSION"
echo "Environment: JAVA_HOME=$JAVA_HOME"
echo "             MVN_HOME=$MVN_HOME"
echo "$KV_VERSION" | tee -a ${ROOT_DIR}/kv/${ENVINFOFILE}
echo "==================================================="
echo "" | tee -a ${ROOT_DIR}/kv/${ENVINFOFILE}

# The test.filterOut=" " property is required to run query tests in
# kvtest/kvquery-IT/src/main/resources/cases/idc_xxx/ directories, which are excluded by default
cd ${KV_DIR} && mvn clean && mvn -fn -PIT verify -Dtest.filterOut=" " ${TEST_ARGS}

JENKINS_MASTER_USER=opc
JENKINS_MASTER_BASEDIR=/block1/opc/.jenkins/jobs
if [ "$JENKINS_HOST" != "" ]; then

	BUILDID=$(ssh -l ${JENKINS_MASTER_USER} ${JENKINS_HOST} "cat ${JENKINS_MASTER_BASEDIR}/${TASK_NAME}/nextBuildNumber")
	BUILDID=$(expr $BUILDID - 1)

	LOGLOCATION=${LOG_DIR}/${TASK_NAME}/$BUILDID
	mkdir -p $LOGLOCATION
	HOST_NAME=$(hostname -f)
	echo "Host: ${HOST_NAME}" >>${ROOT_DIR}/kv/${ENVINFOFILE}
	cd $LOGLOCATION/..
	OLDEST_BUILD=$(ls | sort -n | head -1)
	if [ $(($BUILDID - $OLDEST_BUILD)) -gt 30 ]; then
		rm -rf ${OLDEST_BUILD}
	fi
	cd $LOGLOCATION
	echo "Directory: $(pwd)" >>${ROOT_DIR}/kv/${ENVINFOFILE}

	cp -r ${ROOT_DIR}/kv $LOGLOCATION

	ssh ${JENKINS_MASTER_USER}@${JENKINS_HOST} "mkdir -p ${JENKINS_MASTER_BASEDIR}/${TASK_NAME}/workspace/"
	cd ${ROOT_DIR}/kv/ && scp jenkins_changeset.txt ${ENVINFOFILE} ${JENKINS_MASTER_USER}@${JENKINS_HOST}:${JENKINS_MASTER_BASEDIR}/${TASK_NAME}/workspace/
	cd ${KV_DIR} && scp -r ./kvtest/*/target/failsafe-reports ${JENKINS_MASTER_USER}@${JENKINS_HOST}:${JENKINS_MASTER_BASEDIR}/${TASK_NAME}/workspace/

fi
