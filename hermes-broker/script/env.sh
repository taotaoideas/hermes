#!/bin/sh
#################################################
#     Author: huang jie, Date: 2013/03/27       #
#            Environment configuration          #
#################################################

# set jvm startup argument
JAVA_OPTS="-Xms2g \
            -Xmx2g \
            -Xmn1g \
            -XX:PermSize=128m \
            -XX:MaxPermSize=256m \
            -XX:-DisableExplicitGC \
            -Djava.awt.headless=true \
            -Dcom.sun.management.jmxremote.port=8301 \
            -Dcom.sun.management.jmxremote.authenticate=false \
            -Dcom.sun.management.jmxremote.ssl=false \
            -Dfile.encoding=utf-8 \
            -XX:+PrintGC \
            -XX:+PrintGCDetails \
            -XX:+PrintGCDateStamps \
            -Xloggc:../logs/gc.log \
            -XX:-OmitStackTraceInFastThrow \
            -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/opt/app/dump/wxserver \
            -Xdebug -Xrunjdwp:transport=dt_socket,address=8901,server=y,suspend=n \
            "
export JAVA_OPTS=${JAVA_OPTS}