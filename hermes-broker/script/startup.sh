#!/bin/sh

ENV_FILE="./env.sh"
. "${ENV_FILE}"

PID_FILE=../bin/BrokerServer.pid
PID=0
LOG_PATH="../"
LOG_FILE="run.log"

SERVER_DRIVER=com.ctrip.hermes.broker.BrokerServer

SERVER_HOME=..

CLASSPATH=${SERVER_HOME}

CLASSPATH="${CLASSPATH}":"${CLASSPATH}/lib/*":"${SERVER_HOME}/*":"${SERVER_HOME}/conf"
for i in "${SERVER_HOME}"/*.jar; do
   CLASSPATH="${CLASSPATH}":"${i}"
done



start() {
    PID='check_pid'
	echo ${PID}
#    if [ "${PID}" != "" ];  then
#       echo "#######################################################################################"
#       echo "WARN: ${DESC} already started! (pid=${PID})"
#       echo "#######################################################################################"
#    else
        if [ ! -d "${LOG_PATH}" ]; then
            mkdir "${LOG_PATH}"
        fi
        nohup java ${JAVA_OPTS} -classpath ${CLASSPATH} ${SERVER_DRIVER} > "${LOG_PATH}/${LOG_FILE}" 2>&1 &
        echo "BrokerServer Started!"
#    fi
}

stop(){
    serverPID=`jps | grep HermesRestServer | awk '{print $1;" "}'`
    if [ "${serverPID}" == "" ]; then
        echo "no BrokerServer is running"
    else
        kill -9 ${serverPID}
        echo "BrokerServer Stopped"
    fi
}


check_pid() {
 if [ -f "${PID_FILE}" ]; then
      PID=`cat "${PID_FILE}"`
      if [ -n pid ]; then
          echo ${PID}
      fi
  fi
}


_start() {
    java ${JAVA_OPTS} -classpath ${CLASSPATH} ${SERVER_DRIVER}
}


case "$1" in
    start)
        start
	    ;;
	stop)
	    stop
	    ;;
	check_pid)
	    check_pid
	    ;;
    *)
        echo "Usage: $0 {start|check_pid}"
   	    exit 1;
	    ;;
esac
exit 0
