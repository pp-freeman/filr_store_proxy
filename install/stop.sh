#!/bin/sh
cd /home/file_store_file
PID=$(cat pidfile.txt)
if [ ${PID} ]; 
then
    echo 'Application is stpping...'
    echo kill $PID DONE
    kill $PID
else
    echo 'Application is already stopped...'
fi
