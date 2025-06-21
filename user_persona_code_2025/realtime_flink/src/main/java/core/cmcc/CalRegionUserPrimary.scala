//package core
//
//import java.util.Properties
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.{MapStateDescriptor, StateTtlConfig}
//import org.apache.flink.api.common.time.Time
//import org.apache.flink.api.java.io.TextInputFormat
//import org.apache.flink.connector.hbase.sink.{HBaseMutationConverter, HBaseSinkFunction}
//import org.apache.flink.core.fs.Path
//import org.apache.flink.runtime.state.filesystem.FsStateBackend
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
//import org.apache.flink.streaming.api.functions.source.FileProcessingMode
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.util.Collector
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.client.{Mutation, Put}
//
//
//
///**
// * 1.读取kafka中的信令数据
// * 2.获取用户基础数据-离线
// * 3.信令与用户基础数据做融合，广播
// * 4.获取行政区-基站关系数据-离线，关联行政区-基站关系数据
// * 5.基于窗口计算用户画像
// * 6.输出到hbase， key：行政区  value：人员构成
// */
//
///**
// *最基础的版本，直接统计一个区域的用户画像，不用保存用户流入流出的状态
// *该版本的亮点: 1.设置State的生存时间
//              2.开启Checkpoint
//              3.kafkacomsumer设置消费组
//*/
//
//
//
//object CalRegionUserPrimary {
//  def main(args: Array[String]): Unit = {
//    //获取Flink执行环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    //设置State的生存时间
//    val ttlConfig = StateTtlConfig.newBuilder(Time.days(1)).build()
//
//    //开启Checkpoint
//    env.enableCheckpointing(1000*60*5)
//    env.setStateBackend(new FsStateBackend("hdfs://bigdata01:9000/flink/chk/CalcRegionUserPersona"))
//
//    import org.apache.flink.api.scala._
//
//    //获取基站信令数据-实时
//    //9b0e2fa8-89e2-4b4c-9087-63afebe0847b|101_00002|39.958087|116.296185|1670468511000
//    val topic = "laccell_sign"
//    val prop = new Properties()
//    prop.setProperty("bootstrap.servers","bigdata01:9092")
//    prop.setProperty("group.id","con1")
//    val kafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
//    kafkaConsumer.setStartFromGroupOffsets()
//    val laccellSignStream = env.addSource(kafkaConsumer)
//    val laccellSignTupStream = laccellSignStream.map(line=>{
//      val arr = line.split("\\|")
//      try {
//        val imsi = arr(0)
//        val laccell = arr(1)
//        (imsi,laccell)
//      }catch {
//        case ex: Exception => ("0","0")
//      }
//    }).filter(_._1 != "0")
//
//
//    //获取用户基础数据-离线
//    //9b0e2fa8-89e2-4b4c-9087-63afebe0847b|0|18
//    val userInfoFilePath = "hdfs://bigdata01:9000/data/userInfo/"
//    val userInfoStream = env.readFile(new TextInputFormat(new Path(userInfoFilePath)),userInfoFilePath,FileProcessingMode.PROCESS_CONTINUOUSLY,1000*10)
//    //userInfoTupStream => (imsi,(gender,age))
//    val userInfoTupStream = userInfoStream.map(line=>{
//      val arr = line.split("\\|")
//      try{
//        val imsi = arr(0)
//        val gender = arr(1)
//        val age = arr(2).toInt
//        (imsi,(gender,age))
//      }catch {
//        case ex: Exception=> ("0",("0",-1))
//      }
//    }).filter(_._2._2 > -1)
//
//    val userInfoMapStateDescriptor = new MapStateDescriptor[String,(String,Int)](
//      "userInfo",
//      classOf[String],
//      classOf[(String,Int)]
//    )
//    userInfoMapStateDescriptor.enableTimeToLive(ttlConfig)
//    val broadcastUserInfoTupStream = userInfoTupStream.broadcast(userInfoMapStateDescriptor)
//
//
//    //第一步：关联基站信令数据和用户基础数据
//    //laccellSignUserInfoStream => (imsi, laccell, gender, age)
//    val laccellSignUserInfoStream = laccellSignTupStream.connect(broadcastUserInfoTupStream).process(new BroadcastProcessFunction[(String, String), (String, (String, Int)), (String, String, String, Int)] {
//      //处理基站信令数据
//      override def processElement(value: (String, String), ctx: BroadcastProcessFunction[(String, String), (String, (String, Int)), (String, String, String, Int)]#ReadOnlyContext, out: Collector[(String, String, String, Int)]): Unit = {
//        val imsi = value._1
//        val laccell = value._2
//        //取出BroadcastState中的数据
//        val broadcastState = ctx.getBroadcastState(userInfoMapStateDescriptor)
//        //关联数据
//        val tupValue = broadcastState.get(imsi)
//        if (tupValue != null) {
//          val gender = tupValue._1
//          val age = tupValue._2
//          out.collect((imsi, laccell, gender, age))
//        }
//      }
//
//      //处理用户基础数据，写入状态中
//      override def processBroadcastElement(value: (String, (String, Int)), ctx: BroadcastProcessFunction[(String, String), (String, (String, Int)), (String, String, String, Int)]#Context, out: Collector[(String, String, String, Int)]): Unit = {
//        //获取BroadcastState
//        val broadcastState = ctx.getBroadcastState(userInfoMapStateDescriptor)
//        //向BroadcastState中写入用户基础数据
//        broadcastState.put(value._1, value._2)
//      }
//    })
//
//
//    //获取行政区-基站关系数据-离线
//    //10001_00001|101_00002
//    val regionCellFilePath = "hdfs://bigdata01:9000/data/regionCell/"
//    val regionCellStream = env.readFile(new TextInputFormat(new Path(regionCellFilePath)),regionCellFilePath,FileProcessingMode.PROCESS_CONTINUOUSLY,1000*10)
//    //regionCellTupStream => (laccell,region_id)
//    val regionCellTupStream = regionCellStream.map(line=>{
//      val arr = line.split("\\|")
//      try{
//        val region_id = arr(0)
//        val laccell = arr(1)
//        //把laccell放在第一位，便于后续关联数据
//        (laccell,region_id)
//      }catch {
//        case ex: Exception => ("0","0")
//      }
//    }).filter(_._1 != "0")
//
//    val regionCellMapStateDescriptor = new MapStateDescriptor[String,String](
//      "regionCell",
//      classOf[String],
//      classOf[String]
//    )
//
//    //enableTimeToLive(ttlConfig)是Kafka中消费者对象的一个方法，用于启用消息过期时间（Time To Live，TTL）功能。当消息在Kafka中存储的时间超过了指定的TTL时间时，就会被视为过期消息并被删除。
//    regionCellMapStateDescriptor.enableTimeToLive(ttlConfig)
//    val broadcastregionCellTupStream = regionCellTupStream.broadcast(regionCellMapStateDescriptor)
//
//    //第二步：关联行政区-基站关系数据
//    //joinStream => (region_id, gender, age)
//    val joinStream = laccellSignUserInfoStream.connect(broadcastregionCellTupStream).process(new BroadcastProcessFunction[(String,String,String,Int),(String,String),(String,String,Int)] {
//      //处理基站信令(包括用户基础数据)数据
//      override def processElement(value: (String, String, String, Int), ctx: BroadcastProcessFunction[(String, String, String, Int), (String, String), (String, String, Int)]#ReadOnlyContext, out: Collector[(String, String, Int)]): Unit = {
//        val laccell = value._2
//        val gender = value._3
//        val age = value._4
//        //取出BroadcastState中的数据
//        val broadcastState = ctx.getBroadcastState(regionCellMapStateDescriptor)
//        //关联数据
//        val region_id = broadcastState.get(laccell)
//        if (region_id != null) {
//          out.collect((region_id, gender, age))
//        }
//      }
//
//      //处理行政区-基站关系数据，写入状态中
//      override def processBroadcastElement(value: (String, String), ctx: BroadcastProcessFunction[(String, String, String, Int), (String, String), (String, String, Int)]#Context, out: Collector[(String, String, Int)]): Unit = {
//        //获取BroadcastState
//        val broadcastState = ctx.getBroadcastState(regionCellMapStateDescriptor)
//        //向BroadcastState中写入行政区-基站关系数据
//        broadcastState.put(value._1, value._2)
//      }
//    })
//
//    //第三步：基于窗口计算用户画像
//    val resStream = joinStream.keyBy(_._1).window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
//        .process(new ProcessWindowFunction[(String,String,Int),(String,(Int,Int,Int,Int,Int)),String,TimeWindow] {
//          override def process(key: String, context: Context, elements: Iterable[(String, String, Int)], out: Collector[(String, (Int, Int, Int, Int, Int))]): Unit = {
//            //区域id
//            val region_id = key
//            //男性数量
//            var man = 0
//            //女性数量
//            var women = 0
//            //10-20岁人员数量
//            var age_10_20 = 0
//            //20-40岁人员数量
//            var age_20_40 = 0
//            //40以上人员数量
//            var age_40 = 0
//
//            //迭代处理同一个区域内的数据
//            val it = elements.iterator
//            while(it.hasNext){
//              val tup = it.next()//(laccell, gender, age)
//              val gender = tup._2
//              val age = tup._3
//              //统计男性、女性数量
//              if(gender.equals("1")){
//                man += 1
//              }else{
//                women += 1
//              }
//              //统计年龄区间数量
//              if(age>= 10 && age <20){
//                age_10_20 += 1
//              }else if(age >=20 && age <40){
//                age_20_40 += 1
//              }else if(age >=40){
//                age_40 += 1
//              }
//            }
//            out.collect((region_id,(man,women,age_10_20,age_20_40,age_40)))
//          }
//        })
//
//    //第四步：将结果输出到HBase中
//    val conf = new Configuration()
//    conf.set("hbase.zookeeper.quorum","bigdata01:2181")
//    conf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase")
//    val hbaseSink = new HBaseSinkFunction[(String,(Int,Int,Int,Int,Int))]("user_persona",
//      conf,
//      new HBaseMutationConverter[(String, (Int, Int, Int, Int, Int))] {
//        //初始化方法，只执行一次
//        override def open(): Unit = {}
//        //将Stream中的数据转化为HBase中的PUT操作
//        override def convertToMutation(record: (String, (Int, Int, Int, Int, Int))): Mutation = {
//          val rowkey = record._1
//          val value = record._2
//          val put = new Put(rowkey.getBytes())
//          put.addColumn("persona".getBytes(),"man".getBytes(),value._1.toString.getBytes())
//          put.addColumn("persona".getBytes(),"women".getBytes(),value._2.toString.getBytes())
//          put.addColumn("persona".getBytes(),"age_10_20".getBytes(),value._3.toString.getBytes())
//          put.addColumn("persona".getBytes(),"age_20_40".getBytes(),value._4.toString.getBytes())
//          put.addColumn("persona".getBytes(),"age_40".getBytes(),value._5.toString.getBytes())
//          put
//        }
//      },
//      100,
//      100,
//      1000
//    )
//    resStream.addSink(hbaseSink)
//
//    env.execute("CalcRegionUserPersona")
//  }
//
//}
