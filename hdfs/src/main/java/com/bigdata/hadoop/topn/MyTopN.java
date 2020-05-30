package com.bigdata.hadoop.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MyTopN {

    // 打成jar包后,上传jar到集群中一台服务器,指定main方法所在class
    // 运行命令 hadoop jar hdfs-1.0-SNAPSHOT.jar com.bigdata.hadoop.mapreduce.MyWordCount
    public static void main(String[] args) {
        try {
            //配置文件true从打包后的resources中获取
            Configuration conf = new Configuration(true);
            //将 -D 参数直接设置到conf属性中,并将非 -D 的参数按顺序封装到String数组中
            String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();

            //自定义job
            Job job = Job.getInstance(conf);
            //必须指定入口方法
            job.setJarByClass(MyTopN.class);
            //指定job名称
            job.setJobName("TopN");

            //这块只能在集群运行,依赖hdfs
            //客户端规划的时候将join的右表(小表称之为右表)cache(缓存)到mapTask出现的节点上
            //以便将数据中的数值类型 映射成 中文类型: 1 -> 北京
            //如果字典表太大,那么就不适合直接缓存整表数据映射,而是需要再来一次mapreduce
            //job.addCacheFile(new Path("/data/input/top_type/top_dict.txt").toUri());

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
