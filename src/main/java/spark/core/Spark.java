package spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public final class Spark {
    private final static  SparkConf conf = new SparkConf().setAppName("Spark params sampler written by java").setMaster("local[*]");
    private final static JavaSparkContext sc = new JavaSparkContext(conf); //其底层就是scala的sparkcontext
    public JavaSparkContext getSc(){
        return sc;
    }
    public void closeSpark(){
        sc.close();
    }
}
