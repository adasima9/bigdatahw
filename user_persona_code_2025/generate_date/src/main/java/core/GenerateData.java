package core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public class GenerateData {
    public static void main(String[] args) throws IOException, InterruptedException {
        List<String> userInfoLines= getDataFromFile("user_info.txt");
        List<String> cellLocLines= getDataFromFile("cell_loc_new.txt");
        //System.out.println(userInfoLines);

        Random r = new Random();
        while(true){
            //随机获取一个用户的imsi
            String ranUserLine =  userInfoLines.get(r.nextInt(userInfoLines.size()));
            String splitUserLine[] = ranUserLine.split("\\|");
            String imsi= splitUserLine[0];

            //随机获取一个小区的cell信息
            String ranCellLocLine =  cellLocLines.get(r.nextInt(cellLocLines.size()));
            String[] list = ranCellLocLine.split("\\|");
            String laccell = list[0];
            String latitude = list[1];
            String longitude = list[2];

            long currTime = System.currentTimeMillis();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String time = formatter.format(currTime);

            String data = imsi + "|" + laccell + "|" + latitude + "|" + longitude + "|" + currTime;

            /*
            记录实时产生的基站信令数据
            logger.info(data);
            */

            System.out.println(data);
            Thread.sleep(50);
        }
    }

    private static List<String> getDataFromFile(String filePath) throws IOException {
        List<String> list = new ArrayList<String>();
        try{
            //从classPath路径读取指定资源的输入流
            InputStreamReader read = new InputStreamReader(Objects.requireNonNull(GenerateData.class.getClassLoader().getResourceAsStream(filePath)));
            BufferedReader br = new BufferedReader(read);
            String line;
            while ((line = br.readLine()) != null) {
                list.add(line);
            }
            br.close();
            read.close();
        }catch(IOException ex)
        {
            System.out.println("文件读取异常");
        }
        return list;
    }
}