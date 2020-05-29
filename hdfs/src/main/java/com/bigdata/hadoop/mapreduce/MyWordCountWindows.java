package com.bigdata.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MyWordCountWindows {

    //window版本,直接运行main方法,不手动上传jar包到linux服务器
    //需特殊处理2步: 知会框架; 指定客户端jar上传路径
    public static void main(String[] args) {
        try {
            //配置文件true从打包后的resources中获取
            Configuration conf = new Configuration(true);

            //windows 特殊处理1
            // 如果是在windows(异构平台)上运行
            // 需要让框架知道,使用 .cmd 文件执行而不是 .sh
            conf.set("mapreduce.app-submission.cross-platform", "true");

            //自定义job
            Job job = Job.getInstance(conf);

            //windows 特殊处理2
            //需要告诉客户端上传哪个jar包给集群去执行
            job.setJar("D:\\java\\code\\naruto\\bigdata\\hdfs\\target\\hdfs-1.0-SNAPSHOT.jar");

            //必须指定入口方法
            job.setJarByClass(MyWordCountWindows.class);
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
