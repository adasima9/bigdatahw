package udf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
/**
 */
public class RoaringBitMapByteUDAF extends UserDefinedAggregateFunction {
    /**
     * 聚合函数的输入数据结构
     */
    @Override
    public StructType inputSchema() {
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("field", DataTypes.IntegerType, true));
        return DataTypes.createStructType(structFields);
    }

    /**
     * 聚合缓冲区内的数据结构
     */
    @Override
    public StructType bufferSchema() {
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("field", DataTypes.BinaryType, true));
        return DataTypes.createStructType(structFields);
    }

    /**
     * 聚合函数返回值数据结构
     */
    @Override
    public DataType dataType() {
        return DataTypes.BinaryType;
    }

    /**
     * 聚合函数是否是幂等的，即相同输入是否总是能得到相同输出
     */
    @Override
    public boolean deterministic() {
        //是否强制每次执行的结果相同
        return true;
    }

    /**
     * 初始化缓冲区
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        //初始化
        buffer.update(0, null);
    }

    /**
     *  给聚合函数传入一条新数据进行处理
     */

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // 相同的executor间的数据合并
        Object in = input.get(0);
        Object out = buffer.get(0);
        RoaringBitmap outRR = new RoaringBitmap();
        // 1. 输入为空直接返回不更新
        if(in == null){
            return ;
        }

        // 2. 源为空则直接更新值为输入
        int inInt = Integer.valueOf(in.toString());
        byte[] inBytes = null ;
        if(out == null){
            outRR.add(inInt);
            try{
                // 将RoaringBitmap的数据转成字节数组
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream ndos = new DataOutputStream(bos);
                outRR.serialize(ndos);
                inBytes = bos.toByteArray();
                ndos.close();
            }   catch (IOException e) {
                e.printStackTrace();
            }
            buffer.update(0, inBytes);
            return ;
        }
        // 3. 源和输入都不为空使用 bitmap去重合并
        byte[] outBytes = (byte[]) buffer.get(0);
        byte[] result = outBytes;
        try {
            outRR.deserialize(new DataInputStream(new ByteArrayInputStream(outBytes)));
            outRR.add(inInt);
            ByteArrayOutputStream boss = new ByteArrayOutputStream();
            DataOutputStream ndosn = new DataOutputStream(boss);
            outRR.serialize(ndosn);
            result = boss.toByteArray();
            ndosn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.update(0, result);
    }


    /**
     *  合并聚合函数缓冲区
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        RoaringBitmap inRBM = new RoaringBitmap();
        RoaringBitmap outRBM = new RoaringBitmap();
        Object out = buffer1.get(0);
        byte[] inBytes = (byte[]) buffer2.get(0);
        if(out == null){
            buffer1.update(0, inBytes);
            return ;
        }
        byte[] outBitBytes = (byte[]) out;
        byte[] resultBit = outBitBytes;
        if (out != null) {
            try {
                outRBM.deserialize(new DataInputStream(new ByteArrayInputStream(outBitBytes)));
                inRBM.deserialize(new DataInputStream(new ByteArrayInputStream(inBytes)));
                RoaringBitmap rror = RoaringBitmap.or(outRBM, inRBM) ;
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream ndosn = new DataOutputStream(bos);
                rror.serialize(ndosn);
                resultBit = bos.toByteArray();
                ndosn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            buffer1.update(0, resultBit);
        }
    }

    /**
     * 计算最终结果
     */

    @Override
    public Object evaluate(Row buffer) {
        //根据Buffer计算结果
        Object val = buffer.get(0);
        byte[] outBitBytes = (byte[]) val;
        return outBitBytes;
    }
}
