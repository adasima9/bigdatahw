package core.cmcc

import `case`._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.connector.hbase.sink.{HBaseMutationConverter, HBaseSinkFunction}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Put}

import java.util.Properties

object RegionFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. Kafka 信令流
    val topic = "xdr"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "bigdata01:9092")
    prop.setProperty("group.id", "con1")
    val kafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
    val xdrStream = env.addSource(kafkaConsumer)
      .map(x => XDR.fromKafka(x))
      .filter(_ != null)

    // 2. 用户基础数据流
    val userInfoFilePath = "generate_date/src/main/resources/user_info.txt"
    val userInfoStream = env.readFile(
      new TextInputFormat(new Path(userInfoFilePath)),
      userInfoFilePath,
      FileProcessingMode.PROCESS_CONTINUOUSLY,
      10000
    ).map(line=>UserInfo.fromStr(line))

    // 3. 区域-基站映射
    val regionCellFilePath = "generate_date/src/main/resources/region_cell_new.txt"
    val regionCellStream = env.readFile(
      new TextInputFormat(new Path(regionCellFilePath)),
      regionCellFilePath,
      FileProcessingMode.PROCESS_CONTINUOUSLY,
      2000
    ).map(line=>RegionCell.fromStr(line))

    // 4. Broadcast 用户信息
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
            val gender = tupValue.gender
            val age = tupValue.age
            out.collect(UserRegionInfo(imsi, laccell, gender, age))
          }
        }

        //处理用户基础数据
        override def processBroadcastElement(value: UserInfo, ctx: BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo]#Context, out: Collector[UserRegionInfo]): Unit = {
          //获取BroadcastState
          val broadcastState = ctx.getBroadcastState(userInfoStateDescriptor)
          //向BroadcastState中写入用户基础数据
          broadcastState.put(value.imsi, value)
        }
      })

    // 5. Broadcast 区域基站
    val regionCellStateDescriptor = new MapStateDescriptor[String, String]("regionCell", classOf[String], classOf[String])
    val broadcastRegionCell = regionCellStream.broadcast(regionCellStateDescriptor)

    val userRegionFlowStream = laccellUserInfoStream.connect(broadcastRegionCell)
      .process(new BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow] {
        override def processElement(
                                     value: UserRegionInfo,
                                     ctx: BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow]#ReadOnlyContext,
                                     out: Collector[UserRegionFlow]
                                   ): Unit = {
          val regionId = ctx.getBroadcastState(regionCellStateDescriptor).get(value.laccell)
          if (regionId != null) {
            out.collect(UserRegionFlow(value.imsi, 1, regionId, value.gender, value.age))
          }
        }

        override def processBroadcastElement(
                                              value: RegionCell,
                                              ctx: BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow]#Context,
                                              out: Collector[UserRegionFlow]
                                            ): Unit = {
          ctx.getBroadcastState(regionCellStateDescriptor).put(value.laccell, value.regionId)
        }
      })

    // 6. 原始人口画像窗口统计
//    val resStream = userRegionFlowStream
//      .keyBy(_.regionId)
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
//      .process(new ProcessWindowFunction[UserRegionFlow, RegionCount, String, TimeWindow] {
//        override def process(key: String, context: Context, elements: Iterable[UserRegionFlow], out: Collector[RegionCount]): Unit = {
//          val (man, women, age_10_20, age_20_40, age_40) = elements.foldLeft((0, 0, 0, 0, 0)) {
//            case ((m, w, a10, a20, a40), e) =>
//              val gender = if (e.gender == 1) (m + 1, w, a10, a20, a40) else (m, w + 1, a10, a20, a40)
//              val ageStat = e.age match {
//                case age if age >= 10 && age < 20 => (gender._1, gender._2, a10 + 1, a20, a40)
//                case age if age >= 20 && age < 40 => (gender._1, gender._2, a10, a20 + 1, a40)
//                case age if age >= 40 => (gender._1, gender._2, a10, a20, a40 + 1)
//                case _ => (gender._1, gender._2, a10, a20, a40)
//              }
//              ageStat
//          }
//          val pepCnt = man + women
//          out.collect(RegionCount(key, man, women, age_10_20, age_20_40, age_40, pepCnt))
//        }
//      })
//
//    resStream.print()

    // 7. 写入 region_person 表
    val conf = new Configuration()
    conf.set("hbase.zookeeper.quorum", "bigdata01:2181")
    conf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase")

//    val hbaseSink1 = new HBaseSinkFunction[RegionCount](
//      "region_person",
//      conf,
//      new HBaseMutationConverter[RegionCount] {
//        override def open(): Unit = {}
//
//        override def convertToMutation(record: RegionCount): Mutation = {
//          val put = new Put(record.regionId.getBytes())
//          put.addColumn("person".getBytes(), "man".getBytes(), record.manNum.toString.getBytes())
//          put.addColumn("person".getBytes(), "women".getBytes(), record.womanNum.toString.getBytes())
//          put.addColumn("person".getBytes(), "age_10_20".getBytes(), record.age_10_20.toString.getBytes())
//          put.addColumn("person".getBytes(), "age_20_40".getBytes(), record.age_20_40.toString.getBytes())
//          put.addColumn("person".getBytes(), "age_40".getBytes(), record.age_40.toString.getBytes())
//          put.addColumn("person".getBytes(), "pepCnt".getBytes(), record.pepCnt.toString.getBytes())
//        }
//      },
//      100, 100, 1000
//    )
//    resStream.addSink(hbaseSink1)

    // 8. 读取目标监控区域
    val targetRegionPath = "generate_date/src/main/resources/target_region.txt"
    val targetRegionStream = env.readFile(
      new TextInputFormat(new Path(targetRegionPath)),
      targetRegionPath,
      FileProcessingMode.PROCESS_CONTINUOUSLY,
      10000
    ).map(_.trim)

    val targetRegionStateDescriptor = new MapStateDescriptor[String, Boolean]("targetRegions", classOf[String], classOf[Boolean])
    val broadcastTargetRegionStream = targetRegionStream.broadcast(targetRegionStateDescriptor)

    // 9. 用户进出事件流
    val regionEventStream = userRegionFlowStream
      .keyBy(_.imsi) // KeyedStream[String, UserRegionFlow]
      .connect(broadcastTargetRegionStream)
      .process(new KeyedBroadcastProcessFunction[String, UserRegionFlow, String, UserRegionFlow] {

        lazy val lastRegionState: ValueState[String] = getRuntimeContext.getState(
          new ValueStateDescriptor[String]("lastRegionState", classOf[String])
        )

        override def processElement(
                                     value: UserRegionFlow,
                                     ctx: KeyedBroadcastProcessFunction[String, UserRegionFlow, String, UserRegionFlow]#ReadOnlyContext,
                                     out: Collector[UserRegionFlow]
                                   ): Unit = {
          val currentRegion = value.regionId
          val targetRegions = ctx.getBroadcastState(targetRegionStateDescriptor)
//          if (!targetRegions.contains(currentRegion)) return

          val lastRegion = lastRegionState.value()
          if (lastRegion != currentRegion) {
            if (lastRegion != null && targetRegions.contains(lastRegion)) {
              out.collect(UserRegionFlow(value.imsi, 0, lastRegion, value.gender, value.age)) // 离开
            }
            if (targetRegions.contains(currentRegion)){
              out.collect(UserRegionFlow(value.imsi, 1, currentRegion, value.gender, value.age)) // 进入
            }
            lastRegionState.update(currentRegion)
          }
        }

        override def processBroadcastElement(
                                              value: String,
                                              ctx: KeyedBroadcastProcessFunction[String, UserRegionFlow, String, UserRegionFlow]#Context,
                                              out: Collector[UserRegionFlow]
                                            ): Unit = {
          ctx.getBroadcastState(targetRegionStateDescriptor).put(value, true)
        }
      })
    regionEventStream.print()
    // 10. 聚合统计进出数据
//    def getAgeRange(age: Int): String = {
//      if (age >= 10 && age < 20) "10_20"
//      else if (age >= 20 && age < 40) "20_40"
//      else if (age >= 40) "40_plus"
//      else "under_10"
//    }

//    val inoutAggStream = regionEventStream
//      .keyBy(e => (e.regionId, e.gender, getAgeRange(e.age)))
//      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
//      .apply((key, window, elements, out: Collector[RegionInOutCount]) => {
//        val (regionId, gender, ageRange) = key
//        var inCnt = 0
//        var outCnt = 0
//        for (e <- elements) {
//          if (e.isIn == 1) inCnt += 1
//          else if (e.isIn == 0) outCnt += 1
//        }
//        out.collect(RegionInOutCount(regionId, inCnt, outCnt, gender, ageRange, inCnt + outCnt))
//      })

//    // 11. Sink 写入 HBase 表 region_inout_event
//    val hbaseSink2 = new HBaseSinkFunction[RegionInOutCount](
//      "region_inout_event",
//      conf,
//      new HBaseMutationConverter[RegionInOutCount] {
//        override def open(): Unit = {}
//
//        override def convertToMutation(record: RegionInOutCount): Mutation = {
//          val rowkey = s"${record.regionId}_${System.currentTimeMillis()}"
//          val put = new Put(rowkey.getBytes())
//          put.addColumn("event".getBytes(), "in".getBytes(), record.inCnt.toString.getBytes())
//          put.addColumn("event".getBytes(), "out".getBytes(), record.outCnt.toString.getBytes())
//          put.addColumn("event".getBytes(), "gender".getBytes(), record.gender.toString.getBytes())
//          put.addColumn("event".getBytes(), "age_range".getBytes(), record.ageRange.getBytes())
//          put.addColumn("event".getBytes(), "total".getBytes(), record.total.toString.getBytes())
//          put
//        }
//      },
//      100,
//      100,
//      1000
//    )
//
//    inoutAggStream.addSink(hbaseSink2)

    // 启动作业
    env.execute("CalcRegionUserInfo with InOut Monitor")

  }
}