package com.mrtest.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * map 输出的键值对必须和reducer输入的键值对类型一致
 * @author PXY
 *
 */
public class WordCountMap extends Mapper<LongWritable, Text, IntWritable, Text> {


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        char aa = 3;
        String str = String.valueOf(aa);
        // 我的文件记录的单词是以空格记录单词，所以这里用空格来截取
        String[] words = line.split(str);
        if (!words[0].equals("Evt_Id"))
        {
            if(words.length >= 10)
                context.write(new IntWritable((int)(Float.parseFloat(words[9])/10000.0)),new Text(words[3] +","+words[4]+","+words[8]+","+words[9]));
        }
    }
}
