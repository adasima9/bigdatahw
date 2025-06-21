package core.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class RegionDataLoader {
    private static Map<String, String[]> regionDataMap = new HashMap<>();

    static {
        loadRegionData();
    }

    private static void loadRegionData() {
        String inputFilePath = "region_center_loc.txt"; // 你的输入文件路径，相对于resource文件夹
        try (InputStream inputStream = RegionDataLoader.class.getClassLoader().getResourceAsStream(inputFilePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length == 4) {
                    String regionId = parts[0];
                    String center = parts[1];
                    String boundary = parts[2];
                    String regionName = parts[3];
                    regionDataMap.put(regionId, new String[]{regionName, boundary, center});
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String[] getRegionData(String regionId) {
        return regionDataMap.get(regionId);
    }
}