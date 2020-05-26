package com.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class TestHdfs {

    public Configuration conf = null;

    public FileSystem fs = null;

    @Before
    public void connect() throws Exception {
        // true 表示加载本地代码编译完成后在resources中的配置文件
        conf = new Configuration(true);

        // 通过默认配置创建文件系统子类
        //setFsAuto();

        // 通过自定义配置创建文件系统子类
        setFsUserDefine();

    }

    private void setFsAuto() throws IOException {
        //FileSystem为抽象父类,实际创建的为配置文件scheme中(core-site.xml)配置的子类
/* 注意value的值为ha模式时无端口,区别与单点namenode时的hdfs://node01:9000
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://mycluster</value>
</property>
*/
        // 发现是hdfs,则会使用分布式文件系统子类(DistributedFileSystem)
        // 同时hdfs没有自己的用户系统,会从当前操作系统去获取
        // windows需要配置系统环境变量 HADOOP_USER_NAME
        // linux会取当前操作用户名,用户组
        // 系统更新用户名,组信息后需要在namenode所在服务器通过 hdfs dfsadmin 进行刷新
        fs = FileSystem.get(conf);
    }

    private void setFsUserDefine() throws Exception {
        //自定义文件系统路径, 配置信息, 用户名
        fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "god");
    }

    @Test
    public void mkdir() throws Exception{
        Path dir = new Path("/gin");
        if (fs.exists(dir)){
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);
    }

    @Test
    public void upload() throws Exception {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
        Path outFile = new Path("/gin/out.txt");
        FSDataOutputStream output = fs.create(outFile);

        //从输入流拷贝到输出流(指定配置文件, 是否结束后自动关闭流)
        IOUtils.copyBytes(input, output, conf, true);
    }

    @Test
    public void blocks() throws Exception {
        // 测试准备数据
        // for i in `seq 100000`; do echo "hello gin $i" >> data.txt; done
        // hdfs dfs -D dfs.blocksize=1048576 -put data.txt
        Path file = new Path("/user/god/data.txt");

        FileStatus fas = fs.getFileStatus(file);
        //表示取出文件的所有,从0到最大长度
        BlockLocation[] blks = fs.getFileBlockLocations(fas, 0, fas.getLen());
        for (BlockLocation blk : blks) {
            System.out.println(blk);
        }
        // 输出结果会打印如下信息:块大小,偏移量,所在节点
        // 0,1048576,node03,node02
        // 1048576,540319,node02,node03
        // 分布式计算所依赖的:分布式文件存储,计算向数据移动
        // 其实用户和程序读取的是文件这个级别,并不知道有块的概念
        // 面向文件的输入流 无论怎么读都是从文件开始读起
        FSDataInputStream in = fs.open(file);
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.println();

        //通过seek来实现不同块的数据读取,而不是每次从文件开始读取
        //计算向数据移动后,期望的是分治,只读取自己关心的数据;同时具备距离的概念(优先读取服务器所在的block,本地有不会跨服务器读取)
        //第二块数据
        in.seek(1048576);
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.println();

    }

    @After
    public void close() throws Exception {
        fs.close();
    }

}
