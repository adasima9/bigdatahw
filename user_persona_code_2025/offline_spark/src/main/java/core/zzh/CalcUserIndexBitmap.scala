package core.zzh

import com.clickhouse.jdbc.{ClickHouseConnection, ClickHouseDataSource, ClickHouseStatement}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * @Author zheng-zihong
 * @Date 2024/6/12 14:30
 * @Description:TODO
 */
object CalcUserIndexBitmap {
  def main(args: Array[String]): Unit = {
    //创建sparkSesiion
    val conf: SparkConf = new SparkConf().setAppName("CalcUserIndexBitmap")
    val spark: SparkSession = SparkSession.builder().config(conf).
      master("local[*]").getOrCreate()

    import spark.implicits._
    //读取用户imsi文件,hdfs://bigdata01:9000/data/user_info
    val userIndexDF: RDD[(String, Long)] = spark.read
      .textFile("file:///Users/zhz/Desktop/文档/2.信息技术-平台部位置洞察/5.授课/20250604/数据/user_info.txt")
      .map(line => {
        val strings: Array[String] = line.split("\\|")
        strings(0)
      }).rdd.zipWithUniqueId()
    println(userIndexDF)
    userIndexDF.foreachPartition(itor => {
      //clickhouse连接信息   jdbc:clickhouse://bigdata01:8123
      val url = "jdbc:clickhouse://localhost:8123"
      val prop = new Properties()
      prop.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      prop.setProperty("user", "default")
      prop.setProperty("password", "clickhouse")

      val source = new ClickHouseDataSource(url, prop)
      val conn: ClickHouseConnection = source.getConnection
      val stmt: ClickHouseStatement = conn.createStatement()
      itor.foreach(userindex => {
        val imsi: String = userindex._1
        val indexId: Long = userindex._2
        stmt.executeUpdate(s"insert into USER_INDEX(IMSI,BITMAP_ID) values('${imsi}','${indexId}')")
      })
      stmt.close()
      conn.close()
    })

    spark.close()
    println("程序运行结束")
  }


}
