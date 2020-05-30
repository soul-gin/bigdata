package com.bigdata.hadoop.topn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class MyTopMapper extends Mapper<LongWritable, Text, TopKey, IntWritable> {

    //因为map可能被调起多次,定义在外面减少gc
    //同时,value会发生序列化,变成字节数组进入buffer,不会存在引用数据被修改问题
    TopKey myKey = new TopKey();
    IntWritable myVal = new IntWritable();

    /*//类型字典
    HashMap<String, String> dict = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //从上下文对象中获取缓存文件,封装成字典map
        URI[] files = context.getCacheFiles();
        Path path = new Path(files[0].getPath());
        BufferedReader reader = new BufferedReader(new FileReader(new File(path.getName())));
        String line = reader.readLine();
        while (line != null){
            String[] split = line.split("\t");
            dict.put(split[0], split[1]);
            line = reader.readLine();
        }
    }*/

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //数据样例:  2020-02-29 20:20:20    1   31  -> 时间中间为空格,数据之前为tab制表符
        String[] strs = StringUtils.split(value.toString(), '\t');
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            //设置key
            //key-时间
            Date date = sdf.parse(strs[0]);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            myKey.setYear(cal.get(Calendar.YEAR));
            myKey.setMonth(cal.get(Calendar.MONTH) + 1);
            myKey.setDay(cal.get(Calendar.DAY_OF_MONTH));
            //key-温度
            int temperature = Integer.parseInt(strs[2]);
            myKey.setTemperature(temperature);
            //myKey.setLocation(dict.get(strs[1]));

            //设置value
            myVal.set(temperature);

            //通过上下文,将数据流转下去
            context.write(myKey, myVal);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
