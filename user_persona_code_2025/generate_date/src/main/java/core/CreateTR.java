package core;

import java.io.*;
import java.util.*;
public class CreateTR {
    public static void main(String[] args) {
        // 配置参数（按需修改）
        String inputFile = "generate_date/src/main/resources/region_cell_new.txt";
        String outputFile = "generate_date/src/main/resources/target_region.txt";
        int targetRegions = 50; // 需随机抽取的区域号数量

        try {
            // 1. 逐行提取区域号并去除相邻重复
            List<String> regions = extractUniqueRegions(inputFile);

            // 2. 随机抽取指定数量的区域号
            List<String> sampledRegions = reservoirSampling(regions, targetRegions);

            // 3. 写入目标文件
            writeToFile(sampledRegions, outputFile);

            System.out.println("成功处理！共提取 " + regions.size() + " 个非重复区域号，随机抽取 " +
                    sampledRegions.size() + " 个写入 " + outputFile);
        } catch (IOException e) {
            System.err.println("处理失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 提取区域号并去除相邻重复项
     *
     * @param filePath 输入文件路径
     * @return 无相邻重复的区域号列表
     */
    private static List<String> extractUniqueRegions(String filePath) throws IOException {
        List<String> regions = new ArrayList<>();
        String lastRegion = null; // 记录上一个区域号

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 提取竖线左侧的区域号
                String currentRegion = line.split("\\|")[0].trim();

                // 跳过与上一行重复的区域号[6,7](@ref)
                if (!currentRegion.equals(lastRegion)) {
                    regions.add(currentRegion);
                    lastRegion = currentRegion;
                }
            }
        }
        return regions;
    }

    /**
     * 蓄水池抽样算法随机抽取指定数量元素[2,3](@ref)
     *
     * @param regions 输入列表
     * @param k 抽样数量
     * @return 随机抽取的子集
     */
    private static List<String> reservoirSampling(List<String> regions, int k) {
        if (regions.size() <= k) return new ArrayList<>(regions);

        List<String> reservoir = new ArrayList<>(regions.subList(0, k));
        Random random = new Random();

        for (int i = k; i < regions.size(); i++) {
            int j = random.nextInt(i + 1);
            if (j < k) {
                reservoir.set(j, regions.get(i));
            }
        }
        return reservoir;
    }

    /**
     * 将结果写入文件
     *
     * @param regions 区域号列表
     * @param filePath 输出文件路径
     */
    private static void writeToFile(List<String> regions, String filePath) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (String region : regions) {
                writer.write(region);
                writer.newLine();
            }
        }
    }
}
