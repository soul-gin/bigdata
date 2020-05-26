package com.bigdata.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    /**
     * 相同的key为一组数据,这一组数据调用一次reduce
     *
     * 第一组:
     * hello 1
     * hello 1
     * hello 1
     *
     * 第二组:
     * gin 1
     * gin 1
     * gin 1
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        result.set(sum);
        //单词为key, 次数为value
        context.write(key, result);
    }

}
