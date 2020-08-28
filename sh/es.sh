#!/bin/bash

es_home=/opt/module/elasticsearch
kibana_home=/opt/module/kibana
case $1  in
 "start") {
  for i in hadoop300 hadoop301 hadoop302
  do
  echo "================================== $i 启动es ================================="
    ssh $i  " source /etc/profile;nohup ${es_home}/bin/elasticsearch >/dev/null 2>&1 &"

   done

};;
"stop") {
  for i in hadoop300 hadoop301 hadoop302
  do
  echo "================================== $i 停止es ================================="
      ssh $i "ps -ef|grep $es_home |grep -v grep|awk '{print \$2}'|xargs kill -9" >/dev/null 2>&1
  done

};;
esac