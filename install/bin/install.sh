#!/bin/sh
mkdir -p /home/file_store_file
chmod 777 ../start.sh
chmod 777 ../stop.sh
chmod 777 ../fileStoreProxy.service
\cp -rf ../application.yaml /home/file_store_file
\cp -rf ../file_store_proxy-0.0.1-SNAPSHOT.jar /home/file_store_file
\cp -rf ../testhttps.keystore /home/file_store_file
\cp -rf ../start.sh /home/file_store_file
\cp -rf ../stop.sh /home/file_store_file
\cp -rf ../fileStoreProxy.service /usr/lib/systemd/system/
systemctl daemon-reload
systemctl enable /usr/lib/systemd/system/fileStoreProxy.service
systemctl start fileStoreProxy
