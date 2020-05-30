package com.bigdata.hadoop.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopPartioner extends Partitioner<TopKey, IntWritable> {

    @Override
    public int getPartition(TopKey key, IntWritable value, int numPartitions) {
        //分区器业务逻辑不适合太复杂(每条记录都会调用,耗时长会影响整体耗时)
        //数据量越来越大时需要考虑"数据倾斜"问题
        return key.getYear() % numPartitions;
    }
}
