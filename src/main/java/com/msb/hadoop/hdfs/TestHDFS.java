package com.msb.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class TestHDFS {

    public Configuration conf = null;
    public FileSystem fs = null;

    //C/S
    @Before
    public void conn() throws Exception {
        conf = new Configuration(true);//true
//        fs = FileSystem.get(conf);
//       <property>
//        <name>fs.defaultFS</name>
//        <value>hdfs://mycluster</value>
//       </property>
        //去环境变量 HADOOP_USER_NAME  god
        fs = FileSystem.get(URI.create("hdfs://hdfsnode1:9000/"),conf,"root");


        //conf = new Configuration();
        //fs = FileSystem.get(new URI("hdfs://192.168.197.131:9000"),conf,"root");

    }

    /**
     * 遍历出所有文件元数据信息
     * @throws Exception
     */
    @Test
    public void listAllFile() throws Exception{
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"),true);
        while (iterator.hasNext()){
            LocatedFileStatus ss = iterator.next();
            System.out.println(ss.isFile()?"file":"dir");
            System.out.println(":"+ss.getOwner()+":"+ss.getPath().getName()+":"+ss.getLen());
        }
    }

    /**
     * 增加一个目录
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {

        Path dir = new Path("/msb01");
        if(fs.exists(dir)){
            fs.delete(dir,true);
        }
        fs.mkdirs(dir);

    }

    /**
     * 上传文件
     * @throws Exception
     */
    @Test
    public void upload() throws Exception {

        BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("C:\\Users\\41490\\Desktop\\大数据脑图.png")));
        Path outfile   = new Path("/msb/大数据脑图.png");
        FSDataOutputStream output = fs.create(outfile);

        IOUtils.copyBytes(input,output,conf,true);
    }

    /**
     * 计算向数据移动
     * @throws Exception
     */
    @Test
    public void blocks() throws Exception {

        Path file = new Path("/README.txt");
        FileStatus fss = fs.getFileStatus(file);
        //得到完整 文件的全部block分布情况
        BlockLocation[] blks = fs.getFileBlockLocations(fss, 0, fss.getLen());
        for (BlockLocation b : blks) {
            System.out.println(b);
        }
//        0,        1048576,        node04,node02  A
//        1048576,  540319,         node04,node03  B
        //计算向数据移动~！
        //其实用户和程序读取的是文件这个级别~！并不知道有块的概念~！
        FSDataInputStream in = fs.open(file);  //面向文件打开的输入流  无论怎么读都是从文件开始读起~！

//        blk01: he
//        blk02: llo msb 66231

        in.seek(1000);
        //计算向数据移动后，期望的是分治，只读取自己关心（通过seek实现），同时，具备距离的概念（优先和本地的DN获取数据--框架的默认机制）
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
    }


    /**
     * 打印一个文件
     * @throws Exception
     */
    @Test
    public void read() throws Exception {

        Path file = new Path("/README.txt");
        FSDataInputStream in = fs.open(file);  //面向文件打开的输入流  无论怎么读都是从文件开始读起~！
        do{
            System.out.println(in.readLine());
        }while (in.read()!=-1);
    }











    @After
    public void close() throws Exception {
        fs.close();
    }

}
