//package core
//
//import `case`.{RegionCell, RegionCount, UserInfo, UserRegionFlow, UserRegionInfo, XDR}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
//import org.apache.flink.api.java.io.TextInputFormat
//import org.apache.flink.connector.hbase.sink.{HBaseMutationConverter, HBaseSinkFunction}
//import org.apache.flink.core.fs.Path
//import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}
//import org.apache.flink.streaming.api.functions.source.FileProcessingMode
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.util.Collector
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.client.{Mutation, Put}
//import org.apache.hadoop.hbase.util.Threads.sleep
//import java.util.Properties
//import java.util.Date
//import java.text.SimpleDateFormat
//
///**
// 1.读取kafka中的信令数据
// 2.用户基础数据，基站和区域关系数据
// 3.信令与用户基础数据做融合，广播
// 4.用户出入行政区/广播-基站和区域关系数据，分析用户对每个行政区流入流出情况，保留用户上一个行政区-状态
// 5.按行政区做统计，按行政区keyby，timeserver，每一分钟输出一次统计数据，状态保存行政区的人员构成统计
// 6.输出到hbase， key：行政区  value：人员构成
// */
//
//
//object TestApp {
//  def main(args: Array[String]): Unit = {
//
//    /****************************************第一步：读取kafka中的信令数据********************************************/
//
//    import org.apache.flink.api.scala._   //Flink的Scala API的导入语句，允许用户使用Scala编写Flink应用程序并访问Flink的API
//    val env = StreamExecutionEnvironment.getExecutionEnvironment //创建流处理执行环境
//
//    //获取基站信令数据-实时  9b0e2fa8-89e2-4b4c-9087-63afebe0847b|101_00002|39.958087|116.296185|1670468511000
//    val topic = "xdr"
//    val prop = new Properties()  //创建了一个Properties对象，用于存储Kafka消费者的属性配置
//
//    prop.setProperty("bootstrap.servers","bigdata01:9092") //kafka地址和默认端口号9092
//    prop.setProperty("group.id","con1")                    //设置消费者的group.id属性，即消费者所属的消费组
//    val kafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop) //创建了一个 FlinkKafkaConsumer对象，用于从 Kafka 主题中消费消息。
//    val xdrStream = env.addSource(kafkaConsumer).map(x=>XDR.fromKafka(x)).filter(x=>x!=null)  //其中，String 表示消息的数据类型，SimpleStringSchema()表示消息的反序列化器，prop表示Kafka消费者的属性配置;
//    //xdrStream.print()
//
//
//    /**********************************第二步：用户基础数据，基站和区域关系数据****************************************/
//
//    //获取用户基础数据-离线  9b0e2fa8-89e2-4b4c-9087-63afebe0847b|0|18
//    //val userInfoFilePath = "hdfs://192.168.182.100:9000/data/userInfo" //本地无法识别虚拟机HDFS环境
//    val userInfoFilePath = "C:\\data\\user_info.txt"
//    //使用 env.readFile()方法读取路径userInfoFilePath下的文件，TextInputFormat定义如何读取文件并将其转换为数据流或数据集，将文件解析为文本格式，并返回一个DataStream对象;
//    //FileProcessingMode.PROCESS_CONTINUOUSLY模式下表示每隔10s监视新数据；
//    val userInfoStream = env.readFile(new TextInputFormat(new Path(userInfoFilePath)),userInfoFilePath,FileProcessingMode.PROCESS_CONTINUOUSLY,1000*10).map(line=>UserInfo.fromStr(line))
//
//
//    //获取行政区-基站关系数据-离线  10001_00001|101_00002
//    val regionCellFilePath = "C:\\data\\region_cell.txt"  //"hdfs://bigdata01:9000/data/regionCell"
//    val regionCellStream = env.readFile(new TextInputFormat(new Path(regionCellFilePath)), regionCellFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY,1000 * 10).map(line=>RegionCell.fromStr(line))
//    //regionCellStream.print()
//
//
//
//    /***********************第三步：基站信令数据关联用户基础数据************************************************************/
//    val userInfoMapStateDescriptor = new MapStateDescriptor[String, UserInfo](   //1.定义状态描述器
//      "userInfo",
//      classOf[String],
//      classOf[UserInfo]
//    )
//    val broadcastUserInfoTupStream = userInfoStream.broadcast(userInfoMapStateDescriptor)  //2.广播配置流
//
//    //3.将事件流和广播流进行连接
//    //ctx 是 BroadcastProcessFunction 的上下文对象，用于访问一些运行时的上下文信息; #Context 表示BroadcastProcessFunction 的上下文类型
//    val laccellUserInfoStream = xdrStream.connect(broadcastUserInfoTupStream).process(new BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo] {
//      override def processElement(value: XDR, ctx: BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo]#ReadOnlyContext, out: Collector[UserRegionInfo]): Unit = {
//        val imsi = value.imsi
//        val laccell = value.laccell
//        val broadcastState = ctx.getBroadcastState(userInfoMapStateDescriptor)
//        val tupValue = broadcastState.get(imsi)
//        if (tupValue != null) {
//          val gender = tupValue.gender
//          val age = tupValue.age
//          out.collect(UserRegionInfo(imsi, laccell, gender, age))
//        }
//      }
//
//      //处理用户基础数据，写入广播状态中
//      override def processBroadcastElement(value: UserInfo, ctx: BroadcastProcessFunction[XDR, UserInfo, UserRegionInfo]#Context, out: Collector[UserRegionInfo]): Unit = {
//        //获取BroadcastState
//        val broadcastState = ctx.getBroadcastState(userInfoMapStateDescriptor)
//        //向BroadcastState中写入用户基础数据
//        broadcastState.put(value.imsi, value)
//      }
//
//    })
//    //laccellUserInfoStream.print()
//
//
//
//    /********************第四步：用户基站数据按用户分区关联基站与区域关系广播流数据，分析用户在每个行政区流入流出情况，保留用户上一个行政区状态******************/
//
//    val regionCellMapStateDescriptor = new MapStateDescriptor[String, String](
//      "regionCell",
//      classOf[String], //laccell
//      classOf[String]  //regionID
//    )
//    val broadcastRegionCellStream = regionCellStream.broadcast(regionCellMapStateDescriptor)
//
//    //userRegionFlowStream => (imsi, isIn, region_id, gender, age)
//    val userRegionFlowStream = laccellUserInfoStream.connect(broadcastRegionCellStream).process(new BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow] {
//
//
//      //处理基站信令(包括用户基础数据)数据
//      override def processElement(value: UserRegionInfo, ctx: BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow]#ReadOnlyContext, out: Collector[UserRegionFlow]): Unit = {
//        val imsi= value.imsi
//        val laccell = value.laccell
//        val gender = value.gender
//        val age = value.age
//        //取出BroadcastState中的数据
//        val broadcastState = ctx.getBroadcastState(regionCellMapStateDescriptor)
//        //关联数据
//        val region_id = broadcastState.get(laccell)
//        if(region_id != null){
//            out.collect(UserRegionFlow(imsi, region_id, gender, age))
//        }
//      }
//
//
//      //处理行政区-基站laccell关系数据，写入广播状态中
//      override def processBroadcastElement(value: RegionCell, ctx: BroadcastProcessFunction[UserRegionInfo, RegionCell, UserRegionFlow]#Context, out: Collector[UserRegionFlow]): Unit = {
//        //获取BroadcastState
//        val broadcastState = ctx.getBroadcastState(regionCellMapStateDescriptor)
//        //向BroadcastState中写入行政区-基站关系数据
//        broadcastState.put(value.laccell, value.regionId)
//      }
//    })
//
//    //userRegionFlowStream.print()
//
//
//    /**
//     * 在 Flink 中，处理一分钟窗口数据有多种方法，下面列举其中常见的几种：
//     * 使用window(TumblingEventTimeWindows.of(Time.minutes(1)))方法，该方法会将数据按照 Event Time 分配到 1 分钟大小的窗口中，然后对窗口中的数据进行计算。
//     * 使用window(TumblingProcessingTimeWindows.of(Time.minutes(1)))方法，该方法会将数据按照 Processing Time 分配到 1 分钟大小的窗口中，然后对窗口中的数据进行计算。
//     * 使用window(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(1)))方法，该方法会将所有数据分配到一个全局窗口中，并且每当有一条数据进入窗口时就会触发计算，因此可以实现每分钟计算一次的效果。
//     * 使用window(EventTimeSessionWindows.withGap(Time.minutes(1)))方法，该方法会将数据按照 Event Time 分配到不超过 1 分钟的会话窗口中，当窗口内的数据空闲时间超过 1 分钟时，会触发计算。
//     */
//
//
//    /********************第五步：将数据按照Processing Time分配到1分钟大小的窗口中，然后对窗口中的数据进行计算，计算用户画像******************/
//
//    val resStream = userRegionFlowStream.keyBy(_.regionId).window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))) //minutes
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
//            val tup = it.next()//(isIn, regionID, gender, age)
//            val gender = tup.gender
//            val age = tup.age
//            //统计男性、女性数量
//            if(gender.equals("1")){
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
//
//          out.collect(RegionCount(region_id, man, women, age_10_20, age_20_40, age_40))
//          //println(region_id + ", 开始时间: " + windowStartStr + ", 结束时间: " + windowEndStr + ", " + RegionCount(region_id, man, women, age_10_20, age_20_40, age_40))
//
//        }
//      })
//
//    //resStream.print()
//
//
//
//
//    /********************第六步：数据写入HBASE**********************************************/
//
//    val conf = new Configuration()
//    conf.set("hbase.zookeeper.quorum", "bigdata01:2181")
//    conf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase") //hbase在hdfs上的目录
//
//    val hbaseSink = new HBaseSinkFunction[RegionCount](
//      "region_person",
//      conf,
//      new HBaseMutationConverter[RegionCount] { // mutationConverter负责转换Flink元素为HBaseMutation
//        //初始化方法，只执行一次
//        //open方法是在HBaseMutationConverter实例初始化时调用的，它提供了一个机会来初始化需要的资源：连接池、缓存、配置文件等.
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
//          put
//        }
//      },
//
//      //缓冲区最大大小字节,缓冲区最大写入申请数,缓冲区刷新距离
//      100,
//      100,
//      1000
//    )
//    resStream.addSink(hbaseSink)
//    env.execute("CalcRegionUserInfo")
//
//
//  }
//}