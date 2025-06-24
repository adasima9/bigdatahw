package core.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xuwei
 */
public class HBaseUtil {
    private HBaseUtil(){}

    private static Connection conn;
    static {
        try {
            // 获取hbase的链接
            Configuration conf = new Configuration();
            // 指定hbase使用的zk地址
            // 注意：需要在执行hbase java代码的机器上配置zk和hbase集群的主机名和ip的映射关系
            conf.set("hbase.zookeeper.quorum", "bigdata01:2181");
            // 指定hbase在hdfs上的根目录
            conf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase");
            // 创建HBase数据库链接
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            System.out.println("获取链接失败：" + e.getMessage());
        }
    }

    /**
     * 对外提供的方法
     * @return
     */
    public static Connection getInstance() {
        return conn;
    }

    /**
     * 根据rowkey获取数据
     * @param tableName
     * @param rowKey
     * @return
     * @throws Exception
     */
    public static Map<String, String> getRegionPortraitFromHBase(String tableName, String rowKey) throws IOException {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            HashMap<String, String> resMap = new HashMap<>();
            for (Cell cell : cells) {
                // 列
                byte[] column_bytes = CellUtil.cloneQualifier(cell);
                // 值
                byte[] value_bytes = CellUtil.cloneValue(cell);
                resMap.put(new String(column_bytes), new String(value_bytes));
            }
            return resMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * 获取表中所有regionId列表
     * @param tableName
     * @return
     * @throws Exception
     */
    public static List<String> getAllRegionIds(String tableName) throws IOException {
        List<String> regionIds = new ArrayList<>();
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setCaching(100); // 设置每次扫描的行数

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] rowKey = result.getRow();
                regionIds.add(Bytes.toString(rowKey));
            }
            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
        return regionIds;
    }

    /**
     * 查询指定区域、指定时间内驻留时长大于minStayMinutes的人员明细
     * @param tableName HBase表名
     * @param regionId 区域ID
     * @param produceHour 时间（如2024061010）
     * @param minStayMinutes 驻留时长下限（分钟）
     * @return List<Map<String, Object>> 每个Map包含imsi、gender、age、enter_time、stay_minutes等
     */
    public static List<Map<String, Object>> getRegionStayPersonList(String tableName, String regionId, String produceHour, int minStayMinutes) throws IOException {
        List<Map<String, Object>> resultList = new ArrayList<>();
        String rowKey = regionId + "_" + produceHour;
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String imsi = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            // 假设value为json字符串，包含gender, age, enter_time, stay_minutes
            try {
                // 解析json字符串
                com.alibaba.fastjson.JSONObject obj = com.alibaba.fastjson.JSONObject.parseObject(value);
                int stayMinutes = obj.getIntValue("stay_minutes");
                if (stayMinutes >= minStayMinutes) {
                    Map<String, Object> person = new HashMap<>();
                    person.put("imsi", imsi);
                    person.put("gender", obj.getIntValue("gender"));
                    person.put("age", obj.getIntValue("age"));
                    person.put("enter_time", obj.getString("enter_time"));
                    person.put("stay_minutes", stayMinutes);
                    resultList.add(person);
                }
            } catch (Exception e) {
                // 解析异常，跳过该条
                continue;
            }
        }
        return resultList;
    }

    /**
     * 关闭HBase连接
     */
    public static void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
