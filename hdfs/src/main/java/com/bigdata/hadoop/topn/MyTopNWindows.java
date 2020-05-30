package com.bigdata.hadoop.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MyTopNWindows {

    // 打成jar包后,上传jar到集群中一台服务器,指定main方法所在class
    // 运行命令 hadoop jar hdfs-1.0-SNAPSHOT.jar com.bigdata.hadoop.mapreduce.MyWordCount
    public static void main(String[] args) {
        try {
            //使用本地hadoop配置
            Configuration conf = new Configuration(false);
            //将 -D 参数直接设置到conf属性中,并将非 -D 的参数按顺序封装到String数组中
            String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();

            // 需要让框架知道,windows使用 .cmd 文件执行而不是 .sh
            conf.set("mapreduce.app-submission.cross-platform", "true");
            //将在mapred-site.xml中配置的yarn上运行的配置改为local;
            conf.set("mapreduce.framework.name", "local");


            //自定义job
            Job job = Job.getInstance(conf);

            //window执行jar的路径
            job.setJar("D:\\java\\code\\naruto\\bigdata\\hdfs\\target\\hdfs-1.0-SNAPSHOT.jar");

            //必须指定入口方法
            job.setJarByClass(MyTopNWindows.class);
            //指定job名称
            job.setJobName("TopN");

            //指定输入,输出路径
            TextInputFormat.addInputPath(job, new Path(other[0]));
            //指定输出路径
            Path out = new Path(other[1]);
            if (out.getFileSystem(conf).exists(out)){
                //测试用,实际工作不会轻易删除目录
                out.getFileSystem(conf).delete(out, true);
            }
            TextOutputFormat.setOutputPath(job, out);

            //mapper stage begin
            //mapper class,key,value
            job.setMapperClass(MyTopMapper.class);
            job.setMapOutputKeyClass(TopKey.class);
            job.setMapOutputValueClass(IntWritable.class);

            //partitioner 按 (年,月) 分区
            //分区 -> 满足相同的key获得相同的分区号
            job.setPartitionerClass(TopPartioner.class);
            //sortComparator 排序: (年,月) (温度) 进行排序,且温度为倒序
            job.setSortComparatorClass(TopSortComparator.class);
            //combine
            //job.setCombinerClass();
            //mapper stage end

            //reducer stage begin
            //groupingComparator
            job.setGroupingComparatorClass(TopGroupingComparator.class);
            //reduce
            job.setReducerClass(MyTopReducer.class);

            //指定输出类型,以便序列化
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //reducer stage end

            //提交作业
            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
