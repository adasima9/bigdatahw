//package core.cmcc
//
//import `case`._
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.MapStateDescriptor
//import org.apache.flink.api.java.io.TextInputFormat
//import org.apache.flink.connector.hbase.sink.{HBaseMutationConverter, HBaseSinkFunction}
//import org.apache.flink.core.fs.Path
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
//import org.apache.flink.streaming.api.functions.source.FileProcessingMode
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.util.Collector
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.client.{Mutation, Put}
//import java.text.SimpleDateFormat
//import java.util.{Date, Properties}
//
///**
// * 1.读取kafka中的信令数据
// * 2.用户基础数据，基站和区域关系数据
// * 3.信令与用户基础数据做融合，广播
// * 4.用户出入行政区/广播-基站和区域关系数据，分析用户对每个行政区流入情况
// * 5.按行政区做统计，按行政区keyBy，timeserver，每一分钟输出一次统计数据
// * 6.输出到hbase，key：行政区  value：人员构成
// */
//
//object CalRegionUserTeach {
//  def main(args: Array[String]): Unit = {
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    import org.apache.flink.api.scala._
//
//    //获取基站信令数据-实时  9b0e2fa8-89e2-4b4c-9087-63afebe0847b|101_00002|39.958087|116.296185|1670468511000
//    val topic = "xdr"
//    val prop = new Properties()
//    prop.setProperty("bootstrap.servers","bigdata01:9092") //kafka默认端口号9092
//    prop.setProperty("group.id","con1")
//    val kafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
//    val xdrStream = env.addSource(kafkaConsumer)
//      .map(x=>XDR.fromKafka(x))
//      .filter(x=> x!=null)
//
//    //xdrStream.print()
//
//    //获取用户基础数据-离线  9b0e2fa8-89e2-4b4c-9087-63afebe0847b|0|18
//    val userInfoFilePath = "C:\\Users\\adisma\\Desktop\\第四周\\新建文件夹\\user_persona_code_2025\\generate_date\\src\\main\\resources\\user_info.txt"  //"hdfs://192.168.182.100:9000/data/userInfo"
//    val userInfoStream = env.readFile(
//      new TextInputFormat(new Path(userInfoFilePath)),
//      userInfoFilePath,
//      FileProcessingMode.PROCESS_CONTINUOUSLY,
//      1000*10)
//      .map(line=>UserInfo.fromStr(line))
//
//    //获取行政区-基站关系数据-离线  10001_00001|101_00002
//    val regionCellFilePath = "C:\\Users\\adisma\\Desktop\\第四周\\新建文件夹\\user_persona_code_2025\\generate_date\\src\\main\\resources\\region_cell_new.txt" //"hdfs://bigdata01:9000/data/regionCell/region_cell.txt"
//    val regionCellStream = env.readFile(
//      new TextInputFormat(new Path(regionCellFilePath)),
//      regionCellFilePath,
//      FileProcessingMode.PROCESS_CONTINUOUSLY,
//      1000 * 2)
//      .map(line=>RegionCell.fromStr(line))
//
//    //regionCellStream.print()
//
//
//    //******第一步：关联基站信令数据和用户基础数据******************************************************************************************************
//    val userInfoMapStateDescriptor = new MapStateDescriptor[String, UserInfo](
//      "userInfo",
//      classOf[String],
//      classOf[UserInfo]
//    )
//    val broadcastUserInfoTupStream = userInfoStream.broadcast(userInfoMapStateDescriptor)
//
//    val laccellUserInfoStream = xdrStream
//      .connect(broadcastUserInfoTupStream)
//      .process(new BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo] {
//        override def processElement(value: XDR, ctx: BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo]#ReadOnlyContext, out: Collector[UserRegionInfo]): Unit = {
//          val imsi = value.imsi
//          val laccell = value.laccell
//          val broadcastState = ctx.getBroadcastState(userInfoMapStateDescriptor)
//          val tupValue = broadcastState.get(imsi)
//          if (tupValue != null) {
//            val gender = tupValue.gender
//            val age = tupValue.age
//            out.collect(UserRegionInfo(imsi, laccell, gender, age))
//          }
//        }
//
//        //处理用户基础数据
//        override def processBroadcastElement(value: UserInfo, ctx: BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo]#Context, out: Collector[UserRegionInfo]): Unit = {
//          //获取BroadcastState
//          val broadcastState = ctx.getBroadcastState(userInfoMapStateDescriptor)
//          //向BroadcastState中写入用户基础数据
//          broadcastState.put(value.imsi, value)
//        }
//      })
//    //laccellUserInfoStream.print()
//
//
//    //******第二步：用户基站数据出入行政区、按用户做keyby-广播基站与区域关系数据，分析用户在每个行政区流入情况******************
//    val regionCellMapStateDescriptor = new MapStateDescriptor[String, String](
//      "regionCell",
//      classOf[String], //laccell
//      classOf[String]  //regionID
//    )
//    val broadcastRegionCellStream = regionCellStream.broadcast(regionCellMapStateDescriptor)
//
//    //userRegionFlowStream => (imsi,  region_id, gender, age)
//    val userRegionFlowStream = laccellUserInfoStream.connect(broadcastRegionCellStream)
//      .process(new BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow] {
//        //处理基站信令(包括用户基础数据)数据
//        override def processElement(value: UserRegionInfo, ctx: BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow]#ReadOnlyContext, out: Collector[UserRegionFlow]): Unit = {
//          val imsi= value.imsi
//          val laccell = value.laccell
//          val gender = value.gender
//          val age = value.age
//          //取出BroadcastState中的数据
//          val broadcastState = ctx.getBroadcastState(regionCellMapStateDescriptor)
//          //关联数据
//          val region_id = broadcastState.get(laccell)
//          if(region_id != null){
//              out.collect(UserRegionFlow(imsi, region_id, gender, age))
//          }
//        }
//
//        //处理行政区-基站laccell关系数据，写入广播状态中
//        override def processBroadcastElement(value: RegionCell, ctx: BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow]#Context, out: Collector[UserRegionFlow]): Unit = {
//          //获取BroadcastState
//          val broadcastState = ctx.getBroadcastState(regionCellMapStateDescriptor)
//          //向BroadcastState中写入行政区-基站关系数据
//          broadcastState.put(value.laccell, value.regionId)
//        }
//    })
//
//    //userRegionFlowStream.print()
//
//    //第三步：将数据按照Processing Time分配到1分钟大小的窗口中，然后对窗口中的数据进行计算，计算用户画像;
//    val resStream = userRegionFlowStream
//      .keyBy(_.regionId)
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
//      .process(new ProcessWindowFunction[UserRegionFlow, RegionCount, String, TimeWindow] {
//        override def process(key: String, context: Context, elements: Iterable[UserRegionFlow], out: Collector[RegionCount]): Unit = {
//
//          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//          val windowStart = context.window.getStart // 获取窗口开始时间
//          val windowEnd = context.window.getEnd     // 获取窗口结束时间
//          val windowStartStr = format.format(new Date(windowStart))
//          val windowEndStr = format.format(new Date(windowEnd))
//
//          //区域id
//          val region_id = key
//          //男性数量
//          var man = 0
//          //女性数量
//          var women = 0
//          //10-20岁人员数量
//          var age_10_20 = 0
//          // 20-40岁人员数量
//          var age_20_40 = 0
//          // 40以上人员数量
//          var age_40 = 0
//          // 迭代处理同一个区域内的数据
//          val it = elements.iterator
//
//          while(it.hasNext){
//            val tup = it.next()//(imsi, regionID, gender, age)
//            val gender = tup.gender
//            val age = tup.age
//            //统计男性、女性数量
//            if(gender == 1){
//              man += 1
//            }else{
//              women += 1
//            }
//            //统计年龄区间数量
//            if(age>= 10 && age <20){
//              age_10_20 += 1
//            }else if(age >=20 && age <40){
//              age_20_40 += 1
//            }else if(age >=40){
//              age_40 += 1
//            }
//          }
//          // 统计总人数
//          val pepCnt = man + women
//          out.collect(RegionCount(region_id, man, women, age_10_20, age_20_40, age_40, pepCnt))
//        }
//      })
//
//    resStream.print()
//
//    //******第四步：数据写入HBASE***********************************************************
//    val conf = new Configuration()
//    conf.set("hbase.zookeeper.quorum", "bigdata01:2181")     //HBase依赖ZooKeeper管理集群状态、元数据等
//    conf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase") //hbase在hdfs上的目录
//
//    val hbaseSink = new HBaseSinkFunction[RegionCount](
//      "region_person",
//      conf,
//      new HBaseMutationConverter[RegionCount] { // mutationConverter负责转换Flink元素为HBase Mutation
//        //初始化方法，只执行一次
//        override def open(): Unit = {}
//
//        //将Stream中的数据转化为HBase中的PUT操作
//        override def convertToMutation(record: RegionCount): Mutation = {
//          val rowkey = record.regionId
//          val put = new Put(rowkey.getBytes())
//          put.addColumn("person".getBytes(), "man".getBytes(), record.manNum.toString.getBytes())
//          put.addColumn("person".getBytes(), "women".getBytes(), record.womanNum.toString.getBytes())
//          put.addColumn("person".getBytes(), "age_10_20".getBytes(), record.age_10_20.toString.getBytes())
//          put.addColumn("person".getBytes(), "age_20_40".getBytes(), record.age_20_40.toString.getBytes())
//          put.addColumn("person".getBytes(), "age_40".getBytes(), record.age_40.toString.getBytes())
//          put.addColumn("person".getBytes(), "pepCnt".getBytes(), record.pepCnt.toString.getBytes())
//        }
//      },
//
//      //缓冲区最大大小字节,缓冲区最大写入申请数,缓冲区刷新距离  可根据数据量调整，平衡吞吐量和延迟。
//      100,
//      100,
//      1000
//    )
//    resStream.addSink(hbaseSink)
//
//    env.execute("CalcRegionUserInfo")
//
//  }
//}