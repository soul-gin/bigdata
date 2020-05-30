package com.bigdata.hadoop.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class TopSortComparator extends WritableComparator {

    public TopSortComparator() {
        //告诉父类初始化比较器需要比较的类型是什么
        super(TopKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TopKey k1 = (TopKey)a;
        TopKey k2 = (TopKey)b;

        //年顺序
        int c1 = Integer.compare(k1.getYear(), k2.getYear());
        if (c1 == 0){
            //月顺序
            int c2 = Integer.compare(k1.getMonth(), k2.getMonth());
            if (c2 == 0){
                //温度倒序
                return - Integer.compare(k1.getTemperature(), k2.getTemperature());
            }
            return c2;
        }
        return c1;
    }
}
