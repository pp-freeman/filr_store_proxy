#!/bin/sh
export LANG="en_US.UTF-8"
cd /home/file_store_file
runMessage=`ps aux | grep \`cat pidfile.txt\``
projectStartCommand="java -Dspring.config.location=application.yaml -jar file_store_proxy-0.0.1-SNAPSHOT.jar"
if [[ $runMessage == *$projectStartCommand* ]]
then
    echo "Application has starting ,restarting..."
    kill -9 `cat pidfile.txt`
    nohup java -Dspring.config.location=application.yaml -jar file_store_proxy-0.0.1-SNAPSHOT.jar -java.tmp.dir=/home/java-server/test-demo/temp >/dev/null 2>&1 & echo $! > pidfile.txt
else
    echo "Application has stopped ,starting..."
    nohup java -Dspring.config.location=application.yaml -jar file_store_proxy-0.0.1-SNAPSHOT.jar -java.tmp.dir=/home/java-server/test-demo/temp >/dev/null 2>&1 & echo $! > pidfile.txt
fi
