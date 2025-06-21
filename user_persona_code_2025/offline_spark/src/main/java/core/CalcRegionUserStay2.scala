package core

import com.clickhouse.client.ClickHouseDataType
import com.clickhouse.client.data.ClickHouseBitmap
import com.clickhouse.jdbc.ClickHouseDataSource
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.roaringbitmap.RoaringBitmap
import ru.yandex.clickhouse.settings.ClickHouseQueryParam
import udf.RoaringBitMapByteUDAF
import utils.RoaringBitmapUtil

import java.util.Properties
import scala.collection.mutable

/**
 * 计算每个市区各小时段人群停留数据
 * 执行频率：每小时执行1次
 * Created by xuwei
 */
object CalcRegionUserStay2 {
  def main(args: Array[String]): Unit = {
    //创建SparkSession对象
    val conf = new SparkConf()
      .setMaster("local")
    val sparkSession = SparkSession.builder()
      .appName("CalcRegionUserStay")
      .config(conf)
      .getOrCreate()

    //加载基站信令数据-HDFS
    val laccellSignFilePath = "hdfs://bigdata01:9000/xdr/20280709/19"
    val laccellSignSchema = new StructType()
      .add("imsi",StringType,false)
      .add("laccell",StringType,false)
      .add("latitude",DoubleType,false)
      .add("longitude",DoubleType,false)
      .add("procedureStartTime",LongType,false)
    val laccellSignDf = sparkSession
      .read
      .format("csv")
      .option("sep","|")
      .schema(laccellSignSchema)
      .load(laccellSignFilePath)
    laccellSignDf.createOrReplaceTempView("laccell_sign")


    //加载行政区-基站关系数据-HDFS
    val regionCellFilePath = "hdfs://bigdata01:9000/data/regionCell"
    val regionCellSchema = new StructType()
      .add("region_id",StringType,false)
      .add("laccell",StringType,false)
    val regionCellDf = sparkSession
      .read
      .format("csv")
      .option("sep","|")
      .schema(regionCellSchema)
      .load(regionCellFilePath)
    regionCellDf.createOrReplaceTempView("region_cell")

    //加载用户位图索引数据-ClickHouse
    val url = "jdbc:clickhouse://bigdata01:8123"
    val userBitMapIndexTableName = "USER_INDEX"
    val prop = new Properties()
    prop.setProperty("driver","com.clickhouse.jdbc.ClickHouseDriver")
    prop.setProperty("user","default")
    prop.setProperty("password","clickhouse")
    val userBitMapIndexDf = sparkSession
      .read
      .jdbc(url, userBitMapIndexTableName, prop)
    userBitMapIndexDf.createOrReplaceTempView("user_index")

    //注册自定义函数
    sparkSession.udf.register("mybitmap",new RoaringBitMapByteUDAF())

    //核心业务逻辑SQL
    val execSQL =
      """
        |SELECT
        |	tmp.region_id,
        |	tmp.produce_hour,
        |	collect_set(tmp.BITMAP_ID) as bitmapIds
        |FROM(
        |	SELECT
        |	  rc.region_id,
        |	  from_unixtime(ls.procedureStartTime/1000,'yyyyMMddHH') AS produce_hour,
        |	  ui.BITMAP_ID
        |	FROM laccell_sign AS ls
        |	LEFT JOIN region_cell AS rc ON ls.laccell = rc.laccell
        |	LEFT JOIN user_index AS ui ON ls.imsi = ui.IMSI
        |) AS tmp
        |GROUP BY tmp.region_id,tmp.produce_hour
        |""".stripMargin
    //执行SQL
    val resDf = sparkSession.sql(execSQL)

    //向ClickHouse中写入计算后的区域人群停留数据
    resDf.rdd.foreachPartition(it=>{
      //获取ClickHouse的JDBC连接
      val url = "jdbc:clickhouse://bigdata01:8123"
      val prop = new Properties
      prop.setProperty(ClickHouseQueryParam.USER.getKey, "default")
      prop.setProperty(ClickHouseQueryParam.PASSWORD.getKey, "clickhouse")
      val dataSource = new ClickHouseDataSource(url, prop)
      val conn = dataSource.getConnection

      it.foreach(row=>{
        val region_id = row.getAs[String]("region_id")
        val produce_hour = row.getAs[String]("produce_hour")
        val bmArr = row.getAs[mutable.WrappedArray[java.math.BigDecimal]]("bitmapIds")
        val bitMap = new RoaringBitmap()
        for (x <- bmArr) {
          bitMap.add(x.intValue())
        }

        val ckbitmap = ClickHouseBitmap.wrap(bitMap,ClickHouseDataType.UInt32)
        val stmt = conn.prepareStatement("INSERT INTO  REGION_ID_IMSI_BITMAP(REGION_ID,PRODUCE_HOUR,IMSI_INDEXES) VALUES(?,?,?)")
        stmt.setString(1,region_id)
        stmt.setString(2,produce_hour)
        stmt.setObject(3,ckbitmap)
        stmt.executeUpdate()
        stmt.close()
      })
      conn.close()
    })

    //关闭SparkSession
    sparkSession.stop()
  }

}
