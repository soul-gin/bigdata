package com.bigdata.hadoop.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopGroupingComparator extends WritableComparator {

    public TopGroupingComparator() {
        //告诉父类初始化比较器需要比较的类型是什么
        super(TopKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TopKey k1 = (TopKey)a;
        TopKey k2 = (TopKey)b;

        //分组是"排序"的一种特殊情况, 排序返回 -1 0 1, 分组只需要判断 (-1 1) 和 (0)两种情况
        int c1 = Integer.compare(k1.getYear(), k2.getYear());
        //年相同
        if (c1 == 0){
            //月相同,则为一组
            return Integer.compare(k1.getMonth(), k2.getMonth());
        }
        return c1;
    }
}
