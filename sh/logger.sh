#!/bin/bash

#!/bin/bash
JAVA_BIN=java
PROJECT=/opt/applog
APPNAME=dw-logger-0.0.1-SNAPSHOT.jar
SERVER_PORT=8080

case $1 in
 "start")
   {

    for i in hadoop300 hadoop301 hadoop302
    do
     echo "========: $i==============="
    ssh $i  "$JAVA_BIN -Xms32m -Xmx64m  -jar $PROJECT/$APPNAME --server.port=$SERVER_PORT >/dev/null 2>&1  &"
    done
     echo "========NGINX==============="
    #/usr/local/nginx/sbin/nginx
  };;
  "stop")
  {
     echo "======== NGINX==============="
    #/usr/local/nginx/sbin/nginx  -s stop
    for i in  hadoop300 hadoop301 hadoop302
    do
     echo "========: $i==============="
     ssh $i "ps -ef|grep $APPNAME |grep -v grep|awk '{print \$2}'|xargs kill" >/dev/null 2>&1
    done

  };;
   esac


