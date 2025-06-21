//package core.zzh
//
//import com.clickhouse.client.ClickHouseDataType
//import com.clickhouse.client.data.ClickHouseBitmap
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.apache.spark.storage.StorageLevel
//import org.roaringbitmap.RoaringBitmap
//
//import java.sql.PreparedStatement
//import java.util.Properties
//import scala.collection.mutable
//
///**
// * @Author zheng-zihong
// * @Date 2024/6/12 15:04
// * @Description:TODO
// */
//object CalcUserPersonBitmap {
//  def main(args: Array[String]): Unit = {
//    //创建sparkSesiion
//    val conf: SparkConf = new SparkConf().setAppName("CalcUserIndexBitmap")
//    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").getOrCreate()
//
//    val struct: StructType = new StructType().add("imsi", StringType)
//      .add("gender", IntegerType)
//      .add("age", IntegerType)
//
//    val user_info: DataFrame = spark.read.format("csv").schema(struct).option("sep", "|").
//      load("file:///Users/zhz/Desktop/文档/2.信息技术-平台部位置洞察/5.授课/20250604/数据/user_info.txt")
//    user_info.show(10, false)
//    user_info.createOrReplaceTempView("user_info")
//
//    //读取索引数据
//    val url = "jdbc:clickhouse://localhost:8123"
//    val prop = new Properties()
//    prop.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
//    prop.setProperty("user", "default")
//    prop.setProperty("password", "clickhouse")
//    val table = "USER_INDEX"
//    val user_index: DataFrame = spark.read.jdbc(url, table, prop)
//    user_index.show(10, false)
//    user_index.createOrReplaceTempView("user_index")
//
//    val sql =
//      """
//        |with tmp_res as (
//        |select b.BITMAP_ID,
//        |       case when a.gender=1 then 'boy'
//        |            when a.gender=0 then 'girl'
//        |            else 'none' end as genderFlag,
//        |       case when a.age <= 20 then 'age20down'
//        |            when a.age > 20 and a.age <= 40 then 'age20-40'
//        |            when a.age > 40 then 'age40up' end as ageFlag
//        |from user_info a
//        |join user_index b on a.imsi=b.IMSI
//        |),
//        |tmp_res2 as (
//        |select genderFlag as PORTRAIT_VALUE,collect_set(BITMAP_ID) as PORTRAIT_BITMAP
//        |from tmp_res
//        |group by genderFlag
//        |union
//        |select ageFlag as PORTRAIT_VALUE,collect_set(BITMAP_ID) as PORTRAIT_BITMAP
//        |from tmp_res
//        |group by ageFlag
//        |)
//        |select PORTRAIT_VALUE,PORTRAIT_BITMAP,
//        |        case when PORTRAIT_VALUE = 'boy' then 1
//        |             when PORTRAIT_VALUE = 'girl' then 2
//        |             when PORTRAIT_VALUE = 'age20down' then 3
//        |             when PORTRAIT_VALUE = 'age20-40' then 4
//        |             when PORTRAIT_VALUE = 'age40up' then 5 end as PORTRAIT_ID,
//        |        case when PORTRAIT_VALUE = 'boy' then '男孩'
//        |                          when PORTRAIT_VALUE = 'girl' then '女孩'
//        |                          when PORTRAIT_VALUE = 'age20down' then '年龄小于20岁'
//        |                          when PORTRAIT_VALUE = 'age20-40' then '年龄20到40岁'
//        |                          when PORTRAIT_VALUE = 'age40up' then '年龄40岁以上' end as COMMENT
//        |from tmp_res2
//        |""".stripMargin
//    val userPersonDF: DataFrame = spark.sql(sql)
//    userPersonDF.show(10, false)
//    userPersonDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
//
//    userPersonDF.foreachPartition((itor: Iterator[Row]) => {
//      val conn = ClickHouseUtil.conn()
//      itor.foreach(userPerson => {
//        val PORTRAIT_ID: Int = userPerson.getAs[Int]("PORTRAIT_ID")
//        val COMMENT: String = userPerson.getAs[String]("COMMENT")
//        val PORTRAIT_VALUE: String = userPerson.getAs[String]("PORTRAIT_VALUE")
//        val PORTRAIT_BITMAP: mutable.WrappedArray[java.math.BigDecimal] =
//          userPerson.getAs[mutable.WrappedArray[java.math.BigDecimal]]("PORTRAIT_BITMAP")
//        val bitmap = new RoaringBitmap()
//        PORTRAIT_BITMAP.foreach(id => {
//          bitmap.add(id.intValue())
//        })
//        val bitmapIds: ClickHouseBitmap = ClickHouseBitmap.wrap(bitmap, ClickHouseDataType.UInt32)
//        val stmt: PreparedStatement = conn.
//          prepareStatement("insert into TA_PORTRAIT_IMSI_BITMAP(PORTRAIT_ID,PORTRAIT_VALUE,PORTRAIT_BITMAP,COMMENT) values(?,?,?,?)")
//        stmt.setInt(1, PORTRAIT_ID)
//        stmt.setString(2, PORTRAIT_VALUE)
//        stmt.setObject(3, bitmapIds)
//        stmt.setString(4, COMMENT)
//        stmt.executeUpdate()
//        stmt.close()
//      })
//      conn.close()
//    })
//
//
//    spark.close()
//    println("程序运行结束")
//  }
//
//
//}
