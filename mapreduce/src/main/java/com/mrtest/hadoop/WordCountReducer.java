package com.mrtest.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reducer 输入的键值对必须和map输出的键值对类型一致
 * map <hello,1> <world,1> <hello,1> <apple,1> ....
 * reduce 接收  <apple,[1]> <hello,[1,1]> <world,[1]>
 * @author PXY
 *
 */
public class WordCountReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
                          Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        int count = 0; // 统计总数
        Set<String> dirs = new HashSet<>();
        // 遍历数组，累加求和
        for(Text value : values) {

            // IntWritable类型不能和int类型相加，所以需要先使用get方法转换成int类型
            count += 1;
            dirs.add(value.toString().split(",")[0]);
        }

        // 最后reduce要输出最终的 k v 对
        context.write(key,new Text(String.valueOf(count) + "," + String.valueOf(dirs.size())));
    }
}
