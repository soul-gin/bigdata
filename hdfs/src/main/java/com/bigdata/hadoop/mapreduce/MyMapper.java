package com.bigdata.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    //hadoop框架中,分布式场景,数据需序列化和反序列化
    //hadoop使用了自己的一套,相对jdk的序列化反序列化更加轻量级
    //排序: 比较 -> 这个世界有两种排序(字典序 && 数值顺序)

    //value值
    private final static IntWritable ONE = new IntWritable(1);
    //key
    private Text word = new Text();

    /**
     * hello gin 1
     * 拆分成:
     * hello 1
     * gin 1
     * 1 1
     * 这里的key是值偏移量
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()){
            //根据value值来更新word的值
            word.set(itr.nextToken());
            // word 和 ONE实际为引用传递,如果write没有对word,ONE做序列化(序列化后就是个byte数组,不再是引用),
            // 那么后续修改引用时,其实会修改先放在context中的引用的
            context.write(word, ONE);
        }
    }
}
