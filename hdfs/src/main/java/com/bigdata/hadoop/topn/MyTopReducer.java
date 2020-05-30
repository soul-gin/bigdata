package com.bigdata.hadoop.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MyTopReducer extends Reducer<TopKey, IntWritable, Text, IntWritable> {

    Text rKey = new Text();
    IntWritable rVal = new IntWritable();

    @Override
    protected void reduce(TopKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //数据样例: 需求是取每月温度最高的两天
        // 2020-02-29  33         符合
        // 2020-02-29  32         相同天有更高温度,不符合
        // 2020-02-28  29         符合
        Iterator<IntWritable> iter = values.iterator();

        //第一条数据可以直接获取的标识
        int flag = 0;
        int day = 0;
        //记录第一条数据日期,以便获取第二天
        while (iter.hasNext()){
            //每次获取val,会同步更新 key 的引用,目前key的数据比较全,可以不使用val就完成输出
            IntWritable val = iter.next();
            if (flag == 0) {
                rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
                //rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "\t" + key.getLocation());
                rVal.set(key.getTemperature());
                System.out.println(rKey.toString());
                context.write(rKey, rVal);
                flag++;
                day = key.getDay();
            } else {
                if (day != key.getDay()){
                    rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
                    rVal.set(key.getTemperature());
                    context.write(rKey, rVal);
                    //只需要两天数据,取到第二天就可以跳出了
                    break;
                }
            }
        }

    }
}
