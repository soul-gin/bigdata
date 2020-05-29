package com.bigdata.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MyWordCountWindowsLocal {

    //window版本,直接运行main方法,本地(local)测试程序
    //需特殊处理:
    // 1.安装
    //  在windows下安装hadoop:解压linux,在bin目录替换为window版本(.exe);
    //  修改 hadoop-2.6.5\etc\hadoop 下面的 core-site.xml mapred-site.xml hdfs-site.xml yarn-site.xml
    //  hadoop-env.cmd(配置了JAVA_HOME 环境变量可以不改)
    //  将hadoop.dll放到C:\Windows\System32\hadoop.dll
    //  再配置 HADOOP_HOME 环境变量,PATH添加HADOOP_HOME/bin
    //
    // 2.知会框架为异构平台;
    // 3.将在mapred-site.xml中配置的yarn上运行的配置改为local;
    // 4.指定jar
    public static void main(String[] args) {
        try {
            //使用环境配置
            Configuration conf = new Configuration(false);

            //hadoop解析args参数的util
            //工具类会帮我们把 -D 等等的属性直接set到conf中,会留下 commandOptions
            // 通过 edit configurations 中的 Program arguments 来设置(指定reduces的个数,输入和输出目录)
            // -D mapreduce.job.reduces=2 /data/input/ /data/output/
            GenericOptionsParser parser = new GenericOptionsParser(conf, args);
            String[] otherArgs = parser.getRemainingArgs();

            //windows 特殊处理1
            // 如果是在windows(异构平台)上运行
            // 需要让框架知道,使用 .cmd 文件执行而不是 .sh
            conf.set("mapreduce.app-submission.cross-platform", "true");

            //将在mapred-site.xml中配置的yarn上运行的配置改为local;
            conf.set("mapreduce.framework.name", "local");
            System.out.println("runtime location=" + conf.get("mapreduce.framework.name"));

            //自定义job
            Job job = Job.getInstance(conf);

            //windows 特殊处理2
            //需要告诉客户端上传哪个jar包给集群去执行
            job.setJar("D:\\java\\code\\naruto\\bigdata\\hdfs\\target\\hdfs-1.0-SNAPSHOT.jar");

            //必须指定入口方法
            job.setJarByClass(MyWordCountWindowsLocal.class);
            //指定方法名
            job.setJobName("udfWordCount");
            //指定输入,输出路径
            //目前取的是 Program arguments 中非 -D后面的第一个参数
            System.out.println("args_no_D_0=" + otherArgs[0]);
            Path in = new Path(otherArgs[0]);
            TextInputFormat.addInputPath(job, in);
            //目前取的是 Program arguments 中非 -D后面的第二个参数
            System.out.println("args_no_D_1=" + otherArgs[1]);
            Path out = new Path(otherArgs[1]);
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

            //通过 -D 指定 reduceTasks 的数量
            //job.setNumReduceTasks(2);

            //提交作业
            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
