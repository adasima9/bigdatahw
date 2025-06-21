#!/bin/bash
masterUrl="yarn-cluster"
appName="CalcRegionUserPersona"

#注意：需要将flink脚本路径配置到linux的环境变量中
flink run \
-m ${masterUrl} \
-ynm ${appName} \
-yqu default \
-yjm 1024 \
-ytm 1024 \
-ys 1 \
-p 5 \
-c core.CalcRegionUserPersona \
/data/soft/jobs/realtime_flink/realtime_flink-1.0-SNAPSHOT-jar-with-dependencies.jar