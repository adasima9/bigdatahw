package core

import com.clickhouse.client.ClickHouseDataType
import com.clickhouse.client.data.ClickHouseBitmap
import com.clickhouse.jdbc.ClickHouseDataSource
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.roaringbitmap.RoaringBitmap
import ru.yandex.clickhouse.settings.ClickHouseQueryParam
import udf.RoaringBitMapByteUDAF
import utils.RoaringBitmapUtil

import java.util.Properties
import scala.collection.mutable

/**
 * 整体用户画像数据
 * 执行频率：每天执行1次
 * Created by xuwei
 *
整体思路：
 一、、设定每一类画像的id编号：
1	1	[ClickHouseRoaringBitmap]	男性
2	0	[ClickHouseRoaringBitmap]	女性
3	10	[ClickHouseRoaringBitmap]	10-20岁人员
4	20	[ClickHouseRoaringBitmap]	20-40岁人员
5   40  [ClickHouseRoaringBitmap]  40岁以上人员

 二、确认用户位图id，和用户属性对应关系
 三、确认每一类属性的用户位图
   1、sql目标： 类型id,类型之，用户位图id arr
   2、arr 转位图的udf  ,  id arr --

 四，持久化：  partition中遍历，bitmap处理：id arr -- RoaringBitmap -- ClickHouseBitmap

 */
object CalcUserPersonBitMap2 {
  def main(args: Array[String]): Unit = {
    //创建SparkSession对象
    val conf = new SparkConf()
      .setMaster("local")
    val sparkSession = SparkSession.builder()
      .appName("CalcRegionUserStay")
      .config(conf)
      .getOrCreate()

    //加载用户基础数据-HDFS
    val userInfoFilePath = "hdfs://bigdata01:9000/data/userInfo"
    val userInfoSchema = new StructType()
      .add("imsi",StringType,false)
      .add("gender",StringType,false)
      .add("age",IntegerType,false)
    val userInfoDf = sparkSession
      .read
      .format("csv")
      .option("sep","|")
      .schema(userInfoSchema)
      .load(userInfoFilePath)
    userInfoDf.createOrReplaceTempView("user_info")


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
        |WITH tmp_res AS (
        |	SELECT ui.BITMAP_ID,gender,age_flag
        |	FROM(
        |	  SELECT
        |		imsi,
        |		gender,
        |		CASE
        |		  WHEN (age >=10 AND age < 20) THEN '10'
        |		  WHEN (age >=20 AND age < 40) THEN '20'
        |		  WHEN age >=40 THEN '40'
        |		END AS age_flag
        |	  FROM user_info
        |	  WHERE age >=10
        |	) AS tmp JOIN user_index AS ui
        |	ON tmp.imsi = ui.IMSI
        |)
        |
        --统计男性、女性用户画像数据
        |SELECT
        |  CASE
        |  WHEN gender == '1' THEN 1
        |  WHEN gender == '0' THEN 2
        |  END AS PORTRAIT_ID,
        |  gender AS PORTRAIT_VALUE,
        |  collect_set(BITMAP_ID) AS bitmapIds,
        |  CASE
        |  WHEN gender == '1' THEN '男性'
        |  WHEN gender == '0' THEN'女性'
        |  END AS COMMENT
        |FROM tmp_res
        |GROUP BY gender
        |UNION
        |-- 统计年龄区间用户画像数据
        |SELECT
        |  CASE
        |  WHEN age_flag == '10' THEN 3
        |  WHEN age_flag == '20' THEN 4
        |  WHEN age_flag == '40' THEN 5
        |  END AS PORTRAIT_ID,
        |  age_flag AS PORTRAIT_VALUE,
        |  collect_set(BITMAP_ID) AS bitmapIds,
        |  CASE
        |  WHEN age_flag == '10' THEN '10-20岁人员'
        |  WHEN age_flag == '20' THEN '20-40岁人员'
        |  WHEN age_flag == '40' THEN '40岁以上人员'
        |  END AS COMMENT
        |FROM tmp_res
        |GROUP BY age_flag
        |""".stripMargin

    //执行SQL
    val resDf = sparkSession.sql(execSQL)
//    resDf.show(false)
//    print(resDf.schema)


    //向ClickHouse中写入计算后的用户画像数据
    resDf.rdd.foreachPartition(it=>{
      //获取ClickHouse的JDBC连接
      val url = "jdbc:clickhouse://bigdata01:8123"
      val prop = new Properties
      prop.setProperty(ClickHouseQueryParam.USER.getKey, "default")
      prop.setProperty(ClickHouseQueryParam.PASSWORD.getKey, "clickhouse")
      val dataSource = new ClickHouseDataSource(url, prop)
      val conn = dataSource.getConnection

      it.foreach(row=>{
        val id = row.getAs[Int]("PORTRAIT_ID")

        val value = row.getAs[String]("PORTRAIT_VALUE")
        val bmArr = row.getAs[mutable.WrappedArray[java.math.BigDecimal]]("bitmapIds")
        val bitMap = new RoaringBitmap()
        for(x<-bmArr){
          bitMap.add(x.intValue())
        }
        val comm =row.getAs[String]("COMMENT")
        val ckbitmap = ClickHouseBitmap.wrap(bitMap,ClickHouseDataType.UInt32)
        val stmt = conn.prepareStatement("INSERT INTO TA_PORTRAIT_IMSI_BITMAP(PORTRAIT_ID,PORTRAIT_VALUE,PORTRAIT_BITMAP,COMMENT) VALUES(?,?,?,?)")
        stmt.setInt(1,id)
        stmt.setString(2,value)
        stmt.setObject(3,ckbitmap)
        stmt.setString(4,comm)
        stmt.executeUpdate()
        stmt.close()
      })
      conn.close()
    })

    //关闭SparkSession
    sparkSession.stop()
  }

}
