package core.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import core.bean.Status;
import core.utils.HBaseUtil;
import core.utils.MyDbUtils;
import core.utils.RegionDataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.Array;
import java.util.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 数据接口V1.0
 * Created by xuwei
 */
@RestController//控制器类
@RequestMapping("/v1")//映射路径
public class DataController {
    private static final Logger logger = LoggerFactory.getLogger(DataController.class);
    /**
     * 测试接口
     * @param name
     * @return
     */
    @RequestMapping(value="/t1",method = RequestMethod.GET)
    public Status test(@RequestParam("name") String name) {
        Status status = new Status();
        System.out.println(name);
        return status;
    }
    /**
     * 查询指定区域、指定时间内停留的某类画像的用户总数
     * @param region_id 区域
     * @param produce_hour 时间
     * @param portrait_id 画像id
     * @return 用户总数
     */
    @RequestMapping(value="/userCountByRegionAndTime",method = RequestMethod.GET)
    public JSONObject queryUserCountByRegionAndTime(@RequestParam("region_id") String region_id,@RequestParam("produce_hour") String produce_hour,@RequestParam("portrait_id") int portrait_id) {
        String sql = "WITH (SELECT IMSI_INDEXES FROM REGION_ID_IMSI_BITMAP WHERE REGION_ID = '"+region_id+"' AND PRODUCE_HOUR = '"+produce_hour+"') AS region_time_res \n" +
                "SELECT bitmapCardinality(bitmapAnd(region_time_res,PORTRAIT_BITMAP)) AS res FROM TA_PORTRAIT_IMSI_BITMAP WHERE PORTRAIT_ID = "+portrait_id;
        int cnt = 0;
        List<Object[]> res = MyDbUtils.executeQuerySql(sql);
        if(res.size()==1){
            Object[] objArr = res.get(0);
            cnt = Integer.parseInt(objArr[0].toString());
        }
        JSONObject responseJson = new JSONObject();
        responseJson.put("msg", "");
        responseJson.put("code", 200);
        JSONObject dataJson = new JSONObject();
        dataJson.put("cnt", cnt);
        responseJson.put("data", dataJson);
        return responseJson;
    }

    /**
     * 查询指定区域、指定时间内停留的某类画像的人员列表
     * @param region_id 区域
     * @param produce_hour 时间
     * @param portrait_id 画像ID
     * @return 用户位图索引编号列表
     */
    @RequestMapping(value="/userListByRegionAndTime",method = RequestMethod.GET)
    public JSONObject queryUserListByRegionAndTime(@RequestParam("region_id") String region_id,@RequestParam("produce_hour") String produce_hour,@RequestParam("portrait_id") int portrait_id) {
        String sql = "WITH (SELECT IMSI_INDEXES FROM REGION_ID_IMSI_BITMAP WHERE REGION_ID = '"+region_id+"' AND PRODUCE_HOUR = '"+produce_hour+"') AS region_time_res \n" +
                "SELECT bitmapToArray(bitmapAnd(region_time_res,PORTRAIT_BITMAP)) AS res FROM TA_PORTRAIT_IMSI_BITMAP WHERE PORTRAIT_ID = "+portrait_id;
        List<Object[]> res = MyDbUtils.executeQuerySql(sql);
        ArrayList<Integer> resArr = new ArrayList<Integer>();
        int cnt = 0;
        if(res.size()==1){
            Object[] objArr = res.get(0);
            Object resObj = objArr[0];
            int arrLen = Array.getLength(resObj);
            cnt = arrLen;
            for(int i=0;i<arrLen;i++){
                resArr.add(Integer.parseInt(Array.get(resObj,i).toString()));
            }
        }

        JSONObject responseJson = new JSONObject();
        responseJson.put("msg", "");
        responseJson.put("code", 200);
        JSONObject dataJson = new JSONObject();
        dataJson.put("cnt", cnt);
        dataJson.put("userList", resArr);
        responseJson.put("data", dataJson);
        return responseJson;
    }


    /**
     * 查询指定区域内的用户画像数据。
     * @param region_id
     * @return
     */
    @RequestMapping(value="/regionPortrait",method = RequestMethod.GET)
    public JSONObject queryPortraitByRegion(@RequestParam("region_id") String region_id) {
        JSONObject resJson = new JSONObject();
        try {
            // 初始化返回结果
            resJson.put("code", 200);
            resJson.put("msg", "");
            JSONObject data = new JSONObject();
            resJson.put("data", data);

            // 获取用户画像数据
            Map<String, String> userPersonaMap = HBaseUtil.getRegionPortraitFromHBase("region_person", region_id);
            for (Map.Entry<String, String> entry : userPersonaMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                try {
                    int intValue = Integer.parseInt(value);
                    data.put(key, intValue);
                } catch (NumberFormatException e) {
                    data.put(key, value);
                }
            }

            // 获取区域名称和区域边界信息
            String[] regionData = RegionDataLoader.getRegionData(region_id);
            if (regionData != null) {
                data.put("regionName", regionData[0]);
                data.put("regionId", region_id);
            }
        } catch (Exception e) {
            resJson.put("code", 500);
            resJson.put("msg", "查询失败: " + e.getMessage());
            e.printStackTrace();
        }
        return resJson;
    }

    /**
     * 查询指定区域长时间驻留人群人数。
     * @param regionId
     * @return
     */
    @RequestMapping(value = "/regionStayTotal", method = RequestMethod.GET)
    public JSONObject queryStayTotalByRegion(@RequestParam("region_id") String regionId) {
        JSONObject resJson = new JSONObject();
        try {
            resJson.put("code", 200);
            resJson.put("msg", "");
            JSONObject data = new JSONObject();
            resJson.put("data", data);

            // 查询 HBase 数据
            Map<String, String> regionDataMap = HBaseUtil.getRegionPortraitFromHBase("region_stay_total", regionId);

            // 解析字段（确保值存在才放入）
            if (regionDataMap.containsKey("count")) {
                data.put("count", Integer.parseInt(regionDataMap.get("count")));
            }
            if (regionDataMap.containsKey("ts")) {
                data.put("ts", regionDataMap.get("ts"));
            }

            // 附加 regionId
            data.put("regionId", regionId);

        } catch (Exception e) {
            resJson.put("code", 500);
            resJson.put("msg", "查询失败: " + e.getMessage());
            e.printStackTrace();
        }
        return resJson;
    }

    @RequestMapping(value = "/regionStayDetail", method = RequestMethod.GET)
    public JSONObject queryStayDetailByRegion(
            @RequestParam("region_id") String regionId,
            @RequestParam(value = "limit", defaultValue = "50") int limit) {
        JSONObject resJson = new JSONObject();
        try {
            resJson.put("code", 200);
            resJson.put("msg", "");
            JSONArray data = new JSONArray();
            resJson.put("data", data);

            List<Map<String, String>> records = HBaseUtil.scanByRowPrefix(
                    "region_stay_detail",
                    regionId + "_",
                    limit,
                    "info",
                    Arrays.asList("imsi", "gender", "age", "eventTime", "eventType")
            );
            for (Map<String, String> record : records) {
                JSONObject json = new JSONObject();
                json.putAll(record);
                data.add(json);
            }

        } catch (Exception e) {
            resJson.put("code", 500);
            resJson.put("msg", "查询失败: " + e.getMessage());
            e.printStackTrace();
        }
        return resJson;
    }


//    /**
//     * 获取栅格热力
//     * @return
//     * @para tableName
//     */
//    @RequestMapping(value="/allRegionsHeatMap", method = RequestMethod.GET)
//    public JSONArray queryAllRegionsHeatMap() {
//        JSONArray resultList = new JSONArray();  // 使用JSONArray存储结果
//        try {
//            // 1. 首先获取所有regionId列表
//            List<String> regionIds = HBaseUtil.getAllRegionIds("region_person");
//            // 2. 遍历每个regionId获取数据
//            for (String regionId : regionIds) {
//                Map<String, String> userPersonaMap = HBaseUtil.getRegionPortraitFromHBase("region_person", regionId);
//
//                JSONObject regionData = new JSONObject();
//                regionData.put("regionId", regionId);  // 添加regionId字段
//                Set<Map.Entry<String, String>> entrySet = userPersonaMap.entrySet();
//                for (Map.Entry<String, String> entry : entrySet) {
//                    String key = entry.getKey();
//                    String value = entry.getValue();
//                    regionData.put(key, value);
//                }
//                // 获取区域名称和区域边界信息
//                String[] regionInfo = RegionDataLoader.getRegionData(regionId);
//                if (regionInfo != null) {
//                    regionData.put("regionName", regionInfo[0]);
//                    regionData.put("boundary", regionInfo[1]);
//                    regionData.put("center", regionInfo[2]);
//                }
//                resultList.add(regionData);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return resultList;
//    }

    /**
     * 获取栅格热力
     * @return
     * @para tableName
     */
    @RequestMapping(value="/allRegionsHeatMap", method = RequestMethod.GET)
    public JSONObject queryAllRegionsHeatMap() {
        JSONObject result = new JSONObject();
        try {
            // 初始化返回结果
            result.put("code", 200);
            result.put("msg", "");
            JSONObject data = new JSONObject();
            result.put("data", data);

            // 1. 首先获取所有regionId列表
            List<String> regionIds = HBaseUtil.getAllRegionIds("region_person");

            // 2. 统计所有栅格的画像总和
            JSONObject statis = new JSONObject();
            int totalAge1020 = 0;
            int totalAge40 = 0;
            int totalAge2040 = 0;
            int totalMan = 0;
            int totalWomen = 0;

            JSONArray heatDataList = new JSONArray();
            data.put("statis", statis);
            data.put("heatDataList", heatDataList);

            // 3. 遍历每个regionId获取数据
            for (String regionId : regionIds) {
                Map<String, String> userPersonaMap = HBaseUtil.getRegionPortraitFromHBase("region_person", regionId);

                // 创建当前栅格的数据对象
                JSONObject regionData = new JSONObject();
                regionData.put("regionId", regionId);

                // 统计当前栅格的各年龄段人数
                int age1020 = 0;
                int age40 = 0;
                int age2040 = 0;
                int man = 0;
                int women = 0;
                int pepCnt = 0;

                Set<Map.Entry<String, String>> entrySet = userPersonaMap.entrySet();
                for (Map.Entry<String, String> entry : entrySet) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    try {
                        switch (key) {
                            case "age_10_20":
                                age1020 = Integer.parseInt(value);
                                totalAge1020 += age1020;
                                break;
                            case "age_40":
                                age40 = Integer.parseInt(value);
                                totalAge40 += age40;
                                break;
                            case "age_20_40":
                                age2040 = Integer.parseInt(value);
                                totalAge2040 += age2040;
                                break;
                            case "man":
                                man = Integer.parseInt(value);
                                totalMan += man;
                                break;
                            case "women":
                                women = Integer.parseInt(value);
                                totalWomen += women;
                                break;
                            case "pepCnt":
                                pepCnt = Integer.parseInt(value);
                                break;
                        }
                    } catch (NumberFormatException e) {
                        // 忽略无法解析的数字
                    }
                }

                // 添加到统计中
                statis.put("age_10_20", totalAge1020);
                statis.put("age_40", totalAge40);
                statis.put("age_20_40", totalAge2040);
                statis.put("man", totalMan);
                statis.put("women", totalWomen);

                // 获取区域名称和区域边界信息
                String[] regionInfo = RegionDataLoader.getRegionData(regionId);
                if (regionInfo != null) {
                    // 解析边界数据
                    String[] boundaryPoints = regionInfo[1].split(";");
                    JSONArray boundaryArray = new JSONArray();
                    for (String point : boundaryPoints) {
                        boundaryArray.add(new JSONArray(Arrays.<Object>asList(point.split(","))));
                    }
                    regionData.put("boundary", boundaryArray);
                    regionData.put("center",  Arrays.asList(regionInfo[2].split(",")));
                }
                regionData.put("pepCnt", pepCnt);

                // 添加到热力数据列表
                heatDataList.add(regionData);
            }
        } catch (Exception e) {
            result.put("code", 500);
            result.put("msg", "查询失败: " + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }



}
