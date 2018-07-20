package spark.core;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class paramsSample {
    public static void main(String[] args) {

        /*
         * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
         * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
         * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
         * 只有1G的内存）的初学者
         */

        SparkConf conf =new SparkConf()
                .setAppName("Spark WordCount written by java")
                .setMaster("local[*]");

        /*
         * 第2步：创建SparkContext对象
         * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等
         * 都必须有一个SparkContext(不同的语言具体的类名称不同，如果是java 的为javaSparkContext)
         * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，
         * 包括DAGScheduler、TaskScheduler、SchedulerBackend
         * 同时还会负责Spark程序往Master注册程序等
         * SparkContext是整个Spark应用程序中最为至关重要的一个对象
         */
        JavaSparkContext sc=new JavaSparkContext(conf); //其底层就是scala的sparkcontext

         /*
         * 第3步：根据具体的数据来源（HDFS、HBase、Local FS、DB、S3等）通过SparkContext来创建RDD
         * JavaRDD的创建基本有三种方式：根据外部的数据来源（例如HDFS）、根据Scala集合、由其它的RDD操作
         * 数据会被JavaRDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
         */
        ArrayList<Tuple3<Integer, Integer, Integer>> paramsList =
                new ArrayList<Tuple3<Integer, Integer, Integer>>();
        paramsList.add(new Tuple3<Integer, Integer, Integer>(4,5,4));
        paramsList.add(new Tuple3<Integer, Integer, Integer>(4,5,5));
        paramsList.add(new Tuple3<Integer, Integer, Integer>(4,5,6));
        paramsList.add(new Tuple3<Integer, Integer, Integer>(4,5,7));
        paramsList.add(new Tuple3<Integer, Integer, Integer>(4,5,8));
        JavaRDD<Tuple3<Integer, Integer, Integer>> paramsRDD =sc.parallelize(paramsList);

        /*
         * 第4步：对初始的JavaRDD进行Transformation级别的处理，
         * 例如map、filter等高阶函数等的编程，来进行具体的数据计算
         * 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
         */
        JavaPairRDD<SecondSortKey, String> paramsScore = paramsRDD.mapToPair(
                new PairFunction<Tuple3<Integer, Integer, Integer>,
                        SecondSortKey,
                        String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(Tuple3<Integer, Integer, Integer> param)
                    throws Exception {
                long res = (long)(param._1() + param._2() + param._3());
                String paramString = param._1().toString()+","+param._2().toString()+","+param._3().toString();
                SecondSortKey ssk = new SecondSortKey(paramString, res);
                return new Tuple2<SecondSortKey, String>(ssk, paramString);
            }
        });

        //排序
        JavaPairRDD<SecondSortKey, String> sortByKeyRDD =paramsScore.sortByKey();
        sortByKeyRDD.foreach(new VoidFunction<Tuple2<SecondSortKey,String>>() {
            @Override
            public void call(Tuple2<SecondSortKey,String> s) throws Exception {
                // TODO Auto-generated method stub
                System.out.println("sort0:" + "  " +  s._1.getSecond() + ", " + s._2);
            }
        });

        //过滤自定义的key
        JavaRDD<String> mapRDD = sortByKeyRDD.map(
                new Function<Tuple2<SecondSortKey,String>, String>() {
            @Override
            public String call(Tuple2<SecondSortKey, String> v1) throws Exception {
                return v1._1.getSecond() + "\t" + v1._2;
            }
        });

        mapRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                // TODO Auto-generated method stub
                System.out.println("sort:" + "  " +  s);
            }
        });

        String dir = "result";
        FileUtils fileUtils = new FileUtils();
        File file = new File(dir);
        if(file.exists()){
            if(file.isDirectory()){
                try {
                    fileUtils.deleteDirectory(file);
                } catch (IOException ex){
                    ex.printStackTrace();
                }
            } else{
                file.delete();
            }
        }

        mapRDD.saveAsTextFile(dir);
        sc.close();

        try{
            List<String> list = HdfsOperate.listAll(file.getAbsolutePath());
            for(String remoteFile : list) {//其内部实质上还是调用了迭代器遍历方式，这种循环方式还有其他限制，不建议使用。
                String remoteFile1 = remoteFile;
                String fileName = remoteFile1.replaceAll("file:" + file.getAbsolutePath() +"/", "");
                if(fileName.startsWith("part")){
                    String result = new String(HdfsOperate.readHDFSFile(remoteFile));
                    System.out.println(result);
                    String[] lines = result.split("\n");
                    for(String line : lines){
                        if(!line.isEmpty()){
                            String[] split = line.split("\t");
                            Long score = Long.valueOf(split[0]);
                            String params = split[1];

                            String[] paramList = params.split(",");
                            int WIN = Integer.valueOf(paramList[0]);
                            int PAA = Integer.valueOf(paramList[1]);
                            int ALP = Integer.valueOf(paramList[2]);
                            System.out.println("WIN: "+ WIN + ", PAA: " + PAA + "，ALP: " + ALP + ", score: " + score);
                            //assertEquals(true, result.length() > 0);
                        }
                    }
                }
            }
        } catch(Exception ex){
            ex.printStackTrace();
            assertEquals(true, false);
        }
    }
}
