//package core.zzh
//
//import com.clickhouse.client.ClickHouseDataType
//import com.clickhouse.client.data.ClickHouseBitmap
//import com.clickhouse.jdbc.ClickHouseConnection
//import com.cmcc.utls.ClickHouseUtil
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.functions.{collect_set, from_unixtime}
//import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.roaringbitmap.RoaringBitmap
//
//import java.sql.PreparedStatement
//import java.util.Properties
//import scala.collection.mutable
//
///**
// * @Author zheng-zihong
// * @Date 2024/6/12 16:06
// * @Description:TODO
// */
//object CalcRegionUserStayBitmap {
//  def main(args: Array[String]): Unit = {
//    val hour = try {
//      args(0)
//    } catch {
//      case _: Exception => "2025053014"
//    }
//    println(s"入参为：${hour}")
//    //创建sparkSesiion
//    val conf: SparkConf = new SparkConf().setAppName("CalcUserIndexBitmap")
//    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").getOrCreate()
//    import spark.implicits._
//
//    //读取区域关系表
//    val regionCell: DataFrame = spark.read
//      .textFile(s"file:///Users/zhz/Desktop/文档/2.信息技术-平台部位置洞察/5.授课/20250604/数据/region_cell.txt")
//      .map(line => {
//        val strings: Array[String] = line.split("\\|")
//        (strings(0), strings(1))
//      }).toDF("regionID", "laccell")
//    regionCell.show(10, false)
//    //读取基站信令数据
//    val struct: StructType = new StructType().add("imsi", StringType)
//      .add("laccell", StringType)
//      .add("latitude", DoubleType)
//      .add("longtitude", DoubleType)
//      .add("startTime", LongType)
//    val celllacDF: DataFrame = spark.read.format("csv").schema(struct).option("sep", "|").
//      load(s"file:///Users/zhz/Desktop/文档/2.信息技术-平台部位置洞察/5.授课/20250604/数据/trace_loc/${hour}")
//    celllacDF.show(10, false)
//
//    //读取用户索引
//    val url = "jdbc:clickhouse://localhost:8123"
//    val prop = new Properties()
//    prop.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
//    prop.setProperty("user", "default")
//    prop.setProperty("password", "clickhouse")
//    val table = "USER_INDEX"
//    val user_index: DataFrame = spark.read.jdbc(url, table, prop)
//    user_index.show(10, false)
//
//    val groupResult: DataFrame = celllacDF.as("a").join(regionCell.as("b"), $"a.laccell" === $"b.laccell", "inner")
//      .join(user_index.as("c"), $"a.imsi" === $"c.IMSI", "inner")
//      .select("a.startTime", "b.regionID", "c.BITMAP_ID")
//      .groupBy($"regionID", from_unixtime($"startTime" / 1000, "yyyyMMddHH").as("hourTime"))
//      .agg(collect_set($"BITMAP_ID").as("bitmapIds"))
//    groupResult.show(10, false)
//
//    groupResult.foreachPartition((itor: Iterator[Row]) => {
//      val conn: ClickHouseConnection = ClickHouseUtil.conn()
//      itor.foreach(row => {
//        val regionID: String = row.getAs[String]("regionID")
//        val hourTime: String = row.getAs[String]("hourTime")
//        val PORTRAIT_BITMAP: mutable.WrappedArray[java.math.BigDecimal] = row.getAs[mutable.WrappedArray[java.math.BigDecimal]]("bitmapIds")
//        val bitmap = new RoaringBitmap()
//        PORTRAIT_BITMAP.foreach(id => {
//          bitmap.add(id.intValue())
//        })
//        val bitmapIds: ClickHouseBitmap = ClickHouseBitmap.wrap(bitmap, ClickHouseDataType.UInt32)
//        val stmt: PreparedStatement = conn.prepareStatement("insert into REGION_ID_IMSI_BITMAP(REGION_ID,PRODUCE_HOUR,IMSI_INDEXES) values(?,?,?)")
//        stmt.setString(1, regionID)
//        stmt.setString(2, hourTime)
//        stmt.setObject(3, bitmapIds)
//        stmt.executeUpdate()
//        stmt.close()
//      })
//      conn.close()
//    })
//    spark.close()
//    println("程序运行结束")
//  }
//
//
//}
