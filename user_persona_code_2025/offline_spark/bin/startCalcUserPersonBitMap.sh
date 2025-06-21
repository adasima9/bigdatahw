#!/bin/bash

# 清空ClickHouse中的历史数据
clickhouse-client  \
--password clickhouse  \
--host bigdata01  \
--port 9001  \
--user default  \
--multiquery  \
--query "TRUNCATE TABLE TA_PORTRAIT_IMSI_BITMAP;"


# 执行Spark任务
master="yarn"
deployMode="cluster"
appName="CalcUserPersonBitMap"

spark-submit --master ${master} \
--name ${appName} \
--deploy-mode ${deployMode} \
--queue default \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 2 \
--class core.CalcUserPersonBitMap \
/data/soft/jobs/offline_spark/offline_spark-1.0-SNAPSHOT.jar
