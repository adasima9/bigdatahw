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
          out.collect(value)
          if (value.isIn == 1) {
            regionState.update(value.regionId)
            hasReportedState.update(false)
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 30 * 1000)
          } else if (value.isIn == 0) {
            if (regionState.value() != null && hasReportedState.value()) {
              out.collect(UserRegionFlow(value.imsi, 3, regionState.value(), value.gender, value.age, value.eventTime))
            }
            regionState.clear()
            hasReportedState.clear()
          }
        }
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, UserRegionFlow, UserRegionFlow]#OnTimerContext, out: Collector[UserRegionFlow]): Unit = {
          val regionId = regionState.value()
          val reported = hasReportedState.value()
          if (regionId != null && !reported) {
            out.collect(UserRegionFlow(ctx.getCurrentKey, 2, regionId, 0, 0, ""))
            hasReportedState.update(true)
          }
        }
      })

    val regionStayChangeStream = enrichedRegionEventStream
      .filter(e => e.isIn == 2 || e.isIn == 3)
      .map(e => (e.regionId, if (e.isIn == 2) 1L else -1L))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
      .reduce(
        (a, b) => (a._1, a._2 + b._2),
        (key, window, input, out: Collector[RegionStayCount]) => {
          val record = input.iterator.next()
          out.collect(RegionStayCount(record._1, record._2, window.getStart, window.getEnd))
        }
      )

    regionStayChangeStream.print()

//    val conf = new Configuration()
//    conf.set("hbase.zookeeper.quorum", "bigdata01:2181")
//    conf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase")
//
//    val hbaseSinkStay = new HBaseSinkFunction[RegionStayCount](
//      "region_stay",
//      conf,
//      new HBaseMutationConverter[RegionStayCount] {
//        override def open(): Unit = {}
//        override def convertToMutation(record: RegionStayCount): Mutation = {
//          val rowKey = s"${record.regionId}_${record.windowStart}"
//          val put = new Put(rowKey.getBytes())
//          put.addColumn("stay".getBytes(), "count".getBytes(), record.count.toString.getBytes())
//          put.addColumn("stay".getBytes(), "window_start".getBytes(), record.windowStart.toString.getBytes())
//          put.addColumn("stay".getBytes(), "window_end".getBytes(), record.windowEnd.toString.getBytes())
//          put
//        }
//      },
//      100, 100, 1000
//    )
//
//    regionStayChangeStream.addSink(hbaseSinkStay)

    env.execute("CalcRegionUserInfo with InOut + Stay Detection")
  }
}
