package tool;

import core.GenerateData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Random;

/**
 * 生成基站-经纬度对应关系
 */
public class generateBaseDimLatLot {
    public static void main(String[] args) throws IOException {
        String fileName2 = "region_cell.txt";
        InputStream in2 = GenerateData.class.getClassLoader().getResourceAsStream(fileName2);
        BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
        String line2;
        Random r = new Random();
        while ((line2 = br2.readLine()) != null) {
            String[] splits = line2.split("\\|");
            String laccell = splits[1];

            //随机获取基站纬度
            double latitide = queryHongBao(r,23.37948,23.11335);

            //随机获取基站经度
            double longitude =queryHongBao(r,113.52002,113.18218);

            System.out.println(laccell+"|"+latitide+"|"+longitude);
        }
    }

    /**
    * 获取0.68-6.88之间的随机数
    * @return
    */
    public static Double queryHongBao(Random rand,double MAX,double MIN) {
//        Random rand = new Random();
//        double MAX=6.88;
//        double MIN=0.68;
        double result=0;

        result = MIN + (rand.nextDouble() * (MAX - MIN));
        result = (double) Math.round(result * 100000) / 100000;
        System.out.println(result);

        return result;
    }

}
