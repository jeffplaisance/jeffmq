#!/bin/sh
SCRIPT=`readlink -f $0`
SCRIPTPATH=`dirname $SCRIPT`

CLASSPATH=${SCALA_HOME}/lib/scala-library.jar
for i in ${SCRIPTPATH}/../lib/*.jar
do
  CLASSPATH=${CLASSPATH}:${i}
done

java -Xmx512M -Djava.library.path=/usr/local/lib -Djava.net.preferIPv4Stack=true -cp ${CLASSPATH} jeffmq.JeffMQConsole $@