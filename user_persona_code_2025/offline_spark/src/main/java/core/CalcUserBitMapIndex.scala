package core

import java.util.Properties
import com.clickhouse.jdbc.ClickHouseDataSource
import org.apache.spark.{SparkConf, SparkContext}
import ru.yandex.clickhouse.settings.ClickHouseQueryParam

/**
 * 生成用户位图索引数据
 * 执行频率：每天执行1次
 * Created by xuwei
 */
object CalcUserBitMapIndex {
  def main(args: Array[String]): Unit = {
    //创建SparkContext
    val conf = new SparkConf()
    conf.setAppName("CalcUserBitMapIndex")
    .setMaster("local")
    val sc = new SparkContext(conf)

    //加载数据
    val userInfoFilePath = "I:\\data\\user_info.txt"    //"hdfs://bigdata01:9000/data/userInfo/"
    val textRDD = sc.textFile(userInfoFilePath)

    //解析数据
    val imsiRDD = textRDD.map(line=>{
      val arr = line.split("\\|")
      val imsi = arr(0)
      imsi
    })

    //给数据编号
    val imsiWithIdRDD = imsiRDD.zipWithUniqueId()

    //输出结果数据到ClickHouse中
    imsiWithIdRDD.foreachPartition(it=>{
      val url = "jdbc:clickhouse://bigdata01:8123"
      val prop = new Properties()
      prop.setProperty(ClickHouseQueryParam.USER.getKey, "default")
      prop.setProperty(ClickHouseQueryParam.PASSWORD.getKey, "clickhouse")
      val dataSource = new ClickHouseDataSource(url, prop)
      val conn = dataSource.getConnection
      val stmt = conn.createStatement
      it.foreach(tup=>{
        val imsi = tup._1
        val bitmap_id = tup._2.toInt
        stmt.executeUpdate("INSERT INTO USER_INDEX(IMSI,BITMAP_ID) VALUES('"+imsi+"',"+bitmap_id+")")
      })
      conn.close()
    })
    //关闭SparkContext
    sc.stop()
  }

}
