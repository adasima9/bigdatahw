#!/bin/bash

master="yarn"
deployMode="cluster"
appName="CalcUserBitMapIndex"

spark-submit --master ${master} \
--name ${appName} \
--deploy-mode ${deployMode} \
--queue default \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 2 \
--class core.CalcUserBitMapIndex \
/data/soft/jobs/offline_spark/offline_spark-1.0-SNAPSHOT.jar