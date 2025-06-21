package core.cmcc

import `case`._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.connector.hbase.sink.{HBaseMutationConverter, HBaseSinkFunction}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Put}

object RegionFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val topic = "xdr"
    val prop = new java.util.Properties()
    prop.setProperty("bootstrap.servers", "bigdata01:9092")
    prop.setProperty("group.id", "con1")
    val kafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
    val xdrStream = env.addSource(kafkaConsumer)
      .map(x => XDR.fromKafka(x))
      .filter(_ != null)

    val userInfoFilePath = "generate_date/src/main/resources/user_info.txt"
    val userInfoStream = env.readFile(
      new TextInputFormat(new Path(userInfoFilePath)),
      userInfoFilePath,
      FileProcessingMode.PROCESS_CONTINUOUSLY,
      10000
    ).map(line=>UserInfo.fromStr(line))

    val regionCellFilePath = "generate_date/src/main/resources/region_cell_new.txt"
    val regionCellStream = env.readFile(
      new TextInputFormat(new Path(regionCellFilePath)),
      regionCellFilePath,
      FileProcessingMode.PROCESS_CONTINUOUSLY,
      2000
    ).map(line=>RegionCell.fromStr(line))

    val userInfoStateDescriptor = new MapStateDescriptor[String, UserInfo]("userInfo", classOf[String], classOf[UserInfo])
    val broadcastUserInfo = userInfoStream.broadcast(userInfoStateDescriptor)

    val laccellUserInfoStream = xdrStream.connect(broadcastUserInfo)
      .process(new BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo] {
        override def processElement(value: XDR, ctx: BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo]#ReadOnlyContext, out: Collector[UserRegionInfo]): Unit = {
          val imsi = value.imsi
          val laccell = value.laccell
          val broadcastState = ctx.getBroadcastState(userInfoStateDescriptor)
          val tupValue = broadcastState.get(imsi)
          if (tupValue != null) {
            out.collect(UserRegionInfo(imsi, laccell, tupValue.gender, tupValue.age, value.startTime))
          }
        }

        override def processBroadcastElement(value: UserInfo, ctx: BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo]#Context, out: Collector[UserRegionInfo]): Unit = {
          ctx.getBroadcastState(userInfoStateDescriptor).put(value.imsi, value)
        }
      })

    val regionCellStateDescriptor = new MapStateDescriptor[String, String]("regionCell", classOf[String], classOf[String])
    val broadcastRegionCell = regionCellStream.broadcast(regionCellStateDescriptor)

    val userRegionFlowStream = laccellUserInfoStream.connect(broadcastRegionCell)
      .process(new BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow] {
        override def processElement(value: UserRegionInfo, ctx: BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow]#ReadOnlyContext, out: Collector[UserRegionFlow]): Unit = {
          val regionId = ctx.getBroadcastState(regionCellStateDescriptor).get(value.laccell)
          if (regionId != null) {
            out.collect(UserRegionFlow(value.imsi, 1, regionId, value.gender, value.age, value.eventTime))
          }
        }

        override def processBroadcastElement(value: RegionCell, ctx: BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow]#Context, out: Collector[UserRegionFlow]): Unit = {
          ctx.getBroadcastState(regionCellStateDescriptor).put(value.laccell, value.regionId)
        }
      })

    val targetRegionPath = "generate_date/src/main/resources/target_region.txt"
    val targetRegionStream = env.readFile(
      new TextInputFormat(new Path(targetRegionPath)),
      targetRegionPath,
      FileProcessingMode.PROCESS_CONTINUOUSLY,
      10000
    ).map(_.trim)

    val targetRegionStateDescriptor = new MapStateDescriptor[String, Boolean]("targetRegions", classOf[String], classOf[Boolean])
    val broadcastTargetRegionStream = targetRegionStream.broadcast(targetRegionStateDescriptor)

    val regionEventStream = userRegionFlowStream
      .keyBy(_.imsi)
      .connect(broadcastTargetRegionStream)
      .process(new KeyedBroadcastProcessFunction[String, UserRegionFlow, String, UserRegionFlow] {
        lazy val lastRegionState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastRegionState", classOf[String]))
        override def processElement(value: UserRegionFlow, ctx: KeyedBroadcastProcessFunction[String, UserRegionFlow, String, UserRegionFlow]#ReadOnlyContext, out: Collector[UserRegionFlow]): Unit = {
          val currentRegion = value.regionId
          val targetRegions = ctx.getBroadcastState(targetRegionStateDescriptor)
          val lastRegion = lastRegionState.value()
          if (lastRegion != currentRegion) {
            if (lastRegion != null && targetRegions.contains(lastRegion)) {
              out.collect(UserRegionFlow(value.imsi, 0, lastRegion, value.gender, value.age, value.eventTime))
            }
            if (targetRegions.contains(currentRegion)) {
              out.collect(UserRegionFlow(value.imsi, 1, currentRegion, value.gender, value.age, value.eventTime))
            }
            lastRegionState.update(currentRegion)
          }
        }
        override def processBroadcastElement(value: String, ctx: KeyedBroadcastProcessFunction[String, UserRegionFlow, String, UserRegionFlow]#Context, out: Collector[UserRegionFlow]): Unit = {
          ctx.getBroadcastState(targetRegionStateDescriptor).put(value, true)
        }
      })

    val enrichedRegionEventStream = regionEventStream
      .keyBy(_.imsi)
      .process(new KeyedProcessFunction[String, UserRegionFlow, UserRegionFlow] {
        lazy val regionState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("regionState", classOf[String]))
        lazy val hasReportedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("hasReported", classOf[Boolean]))

        override def processElement(value: UserRegionFlow, ctx: KeyedProcessFunction[String, UserRegionFlow, UserRegionFlow]#Context, out: Collector[UserRegionFlow]): Unit = {
          if (value.isIn == 1) {
            regionState.update(value.regionId)
            hasReportedState.update(false)
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 30 * 1000)
          } else if (value.isIn == 0) {
            if (regionState.value() != null && hasReportedState.value()) {
              out.collect(UserRegionFlow(value.imsi, 3, regionState.value(), value.gender, value.age, value.eventTime)) // 离开长驻
            }
            regionState.clear()
            hasReportedState.clear()
          }
        }

        override def onTimer(timestamp: Long,  ctx: KeyedProcessFunction[String, UserRegionFlow, UserRegionFlow]#OnTimerContext, out: Collector[UserRegionFlow]): Unit = {
          val regionId = regionState.value()
          val reported = hasReportedState.value()
          if (regionId != null && !reported) {
            out.collect(UserRegionFlow(ctx.getCurrentKey, 2, regionId, 0, 0, timestamp.toString)) // 进入长驻
            hasReportedState.update(true)
          }
        }
      })

    val regionStayDeltaStream = enrichedRegionEventStream
      .filter(e => e.isIn == 2 || e.isIn == 3)
      .map(e => (e.regionId, if (e.isIn == 2) 1L else -1L))

    val regionStayTotalStream = regionStayDeltaStream
      .keyBy(_._1)
      .process(new KeyedProcessFunction[String, (String, Long), RegionStayCount] {
        lazy val currentCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentCount", classOf[Long]))

        override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), RegionStayCount]#Context, out: Collector[RegionStayCount]): Unit = {
          val currentCount = Option(currentCountState.value()).getOrElse(0L)
          val updatedCount = currentCount + value._2
          currentCountState.update(updatedCount)
          val now = System.currentTimeMillis()
          out.collect(RegionStayCount(value._1, updatedCount, now, now))
        }
      })

    regionStayTotalStream.print()

    val conf = new Configuration()
    conf.set("hbase.zookeeper.quorum", "bigdata01:2181")
    conf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase")

    val hbaseSinkStayLongCount = new HBaseSinkFunction[RegionStayCount](
      "region_stay_total",
      conf,
      new HBaseMutationConverter[RegionStayCount] {
        override def open(): Unit = {}

        override def convertToMutation(record: RegionStayCount): Mutation = {
          val rowKey = record.regionId.getBytes()
          val put = new Put(rowKey)
          put.addColumn("stay".getBytes(), "count".getBytes(), record.count.toString.getBytes())
          put.addColumn("stay".getBytes(), "ts".getBytes(), record.windowEnd.toString.getBytes())
          put
        }
      },
      100, 100, 1000
    )

    regionStayTotalStream.addSink(hbaseSinkStayLongCount)

    val hbaseSinkStayLongDetail = new HBaseSinkFunction[UserRegionFlow](
      "region_stay_detail",
      conf,
      new HBaseMutationConverter[UserRegionFlow] {
        override def open(): Unit = {}

        override def convertToMutation(record: UserRegionFlow): Mutation = {
          val tsReversed = Long.MaxValue - System.currentTimeMillis()
          val rowKey = s"${record.regionId}_${tsReversed}_${record.imsi}"
          val put = new Put(rowKey.getBytes())
          put.addColumn("info".getBytes(), "imsi".getBytes(), record.imsi.getBytes())
          put.addColumn("info".getBytes(), "gender".getBytes(), record.gender.toString.getBytes())
          put.addColumn("info".getBytes(), "age".getBytes(), record.age.toString.getBytes())
          put.addColumn("info".getBytes(), "eventTime".getBytes(), record.eventTime.getBytes())
          put.addColumn("info".getBytes(), "eventType".getBytes(), record.isIn.toString.getBytes())
          put
        }
      },
      100, 100, 1000
    )

    enrichedRegionEventStream.addSink(hbaseSinkStayLongDetail)

    val hbaseSinkRegionEvent = new HBaseSinkFunction[UserRegionFlow](
      "region_flow_event",
      conf, // HBase configuration
      new HBaseMutationConverter[UserRegionFlow] {
        override def open(): Unit = {}

        override def convertToMutation(record: UserRegionFlow): Mutation = {
          val reversedTs = Long.MaxValue - System.currentTimeMillis()
          val rowKey = s"${record.regionId}_${reversedTs}_${record.imsi}"
          val put = new Put(rowKey.getBytes())
          put.addColumn("info".getBytes(), "imsi".getBytes(), record.imsi.getBytes())
          put.addColumn("info".getBytes(), "gender".getBytes(), record.gender.toString.getBytes())
          put.addColumn("info".getBytes(), "age".getBytes(), record.age.toString.getBytes())
          put.addColumn("info".getBytes(), "eventTime".getBytes(), record.eventTime.getBytes())
          put.addColumn("info".getBytes(), "eventType".getBytes(), record.isIn.toString.getBytes()) // 1 for enter, 0 for leave
          put
        }
      },
      100, 100, 1000
    )

    regionEventStream.addSink(hbaseSinkRegionEvent)

    env.execute("CalcRegionUserInfo with InOut + Stay Detection")
  }
}
