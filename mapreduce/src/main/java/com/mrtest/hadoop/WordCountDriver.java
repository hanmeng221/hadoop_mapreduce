package com.mrtest.hadoop;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 运行主函数
 * @author PXY
 *
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // 获得一个job对象，用来完成一个mapreduce作业
        Job job = Job.getInstance(conf);

        // 让程序找到主入口
        job.setJarByClass(WordCountDriver.class);
        System.out.println(args[0]);
        System.out.println(args[1]);
        // 指定输入数据的目录，指定数据计算完成后输出的目录
        // sbin/yarn jar share/hadoop/xxxxxxx.jar wordcount /wordcount/input/ /wordcount/output/
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 告诉我调用那个map方法和reduce方法
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReducer.class);
        System.out.println("MapReducer");
        // 指定map输出键值对的类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        System.out.println("setValue");
        // 指定reduce输出键值对的类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        System.out.println("set out");
        // 提交job任务
        System.out.println("start the job");
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
