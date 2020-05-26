package com.bigdata.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MyWordCount {

    public static void main(String[] args) {
        try {
            //配置文件true从打包后的resources中获取
            Configuration conf = new Configuration(true);
            //自定义job
            Job job = Job.getInstance(conf);
            //必须指定入口方法
            job.setJarByClass(MyWordCount.class);
            //指定方法名
            job.setJobName("udfWordCount");
            //指定输入,输出路径
            Path in = new Path("/data/wc/input");
            TextInputFormat.addInputPath(job, in);

            Path out = new Path("/data/wc/output");
            if (out.getFileSystem(conf).exists(out)){
                //测试用,实际工作不会轻易删除目录
                out.getFileSystem(conf).delete(out, true);
            }
            TextOutputFormat.setOutputPath(job, out);

            //使用自定义的mapper reducer
            job.setMapperClass(MyMapper.class);
            //指定输出类型,以便序列化
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setReducerClass(MyReducer.class);

            //提交作业
            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
