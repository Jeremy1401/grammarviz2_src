package spark;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.BufferedReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import java.util.List;

import net.seninp.gi.logic.GrammarRules;
import net.seninp.jmotif.sax.NumerosityReductionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.core.*;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;
import net.seninp.util.StackTrace;
import net.seninp.gi.rulepruner.SampledPoint;
import net.seninp.gi.rulepruner.RulePruner;
import net.seninp.gi.GIAlgorithm;
import net.seninp.gi.rulepruner.RulePrunerParameters;
import java.util.Collections;
import net.seninp.gi.rulepruner.ReductionSorter;
import static org.junit.Assert.assertEquals;

public class sparkParams {
    private static final String TEST_DATA_FNAME = "C://Users/dsm/Desktop/grammarviz3-optimized/data/TEK17_SMOOTH.txt";//"/Users/Jeremy/Desktop/毕设/参考文献/导师推荐/代码/grammarviz3-optimized/data/TEK17_SMOOTH.txt";
    private static final String LIMIT_STR = "";

    final static Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final String SPACE = " ";
    private static final String CR = "\n";

    private static final boolean USE_WINDOW_SLIDE = true;
    private static final int WIN_SIZE = 120;
    private static final int PAA_SIZE = 4;
    private static final int ALPHABET_SIZE = 8;
    private static final double NORM_THRESHOLD = 0.05;
    private static final int DISCORDS_TO_TEST = 5;
    private static final NumerosityReductionStrategy STRATEGY = NumerosityReductionStrategy.EXACT;

    private static double[] series;

    private GrammarRules grammarRules;

    private static final Logger LOGGER = LoggerFactory.getLogger(sparkParams.class);

    public static void main(String[] args) {
        // check if everything is ready
        if ((null == TEST_DATA_FNAME) || TEST_DATA_FNAME.isEmpty()) {
            LOGGER.info("unable to load data - no data source selected yet");
            return;
        }

        // make sure the path exists
        Path path = Paths.get(TEST_DATA_FNAME);
        if (!(Files.exists(path))) {
            LOGGER.info("file " + TEST_DATA_FNAME + " doesn't exist.");
            return;
        }

        // read the input
        //
        ArrayList<Double> data = new ArrayList<Double>();

        // lets go
        try {

            // set the lines limit
            long loadLimit = 0l;
            if (!(null == LIMIT_STR) && !(LIMIT_STR.isEmpty())) {
                loadLimit = Long.parseLong(LIMIT_STR);
                LOGGER.info("there is a limit" + LIMIT_STR);
            }

            // open the reader
            BufferedReader reader = Files.newBufferedReader(path, DEFAULT_CHARSET);

            // read by the line in the loop from reader
            String line = null;
            long lineCounter = 0;
            while ((line = reader.readLine()) != null) {
                String[] lineSplit = line.trim().split("\\s+");
                // we read only first column
                // for (int i = 0; i < lineSplit.length; i++) {
                double value = new BigDecimal(lineSplit[0]).doubleValue();
                data.add(value);
                // }
                lineCounter++;
                // break the load if needed
                if ((loadLimit > 0) && (lineCounter > loadLimit)) {
                    break;
                }
            }
            reader.close();
        } catch (Exception e) {
            String stackTrace = StackTrace.toString(e);
            System.err.println(StackTrace.toString(e));
            LOGGER.info("error while trying to read data from "
                    + TEST_DATA_FNAME + ":\n" + stackTrace);
        } finally {
            assert true;
        }

        // convert to simple doubles array and clean the variable
        if (!(data.isEmpty())) {
            series = new double[data.size()];
            for (int i = 0; i < data.size(); i++) {
                series[i] = data.get(i);
            }
        }
        data = new ArrayList<Double>();
        LOGGER.info("loaded " + series.length + " points....");

        // check if the data is loaded
        if (null == series || series.length == 0) {
            LOGGER.info("unable to \"Process data\" - no data were loaded ...");
        } else {
            ArrayList<SampledPoint> res = new ArrayList<SampledPoint>();

            RulePruner rp = new RulePruner(series);
            int[] boundaries = {10, 200, 10, 2, 15, 1, 2, 10, 1};

            LOGGER.info("window range: " + boundaries[0] + " - " + boundaries[1] + ", step " + boundaries[2]);
            LOGGER.info("PAA range: " + boundaries[3] + " - " + boundaries[4] + ", step " + boundaries[5]);
            LOGGER.info("Alphabet range: " + boundaries[6] + " - " + boundaries[7] + ", step " + boundaries[8]);

            // 初始化参数集合
            ArrayList<Tuple3<Integer, Integer, Integer>> paramsList = new ArrayList<>();

            int WIN_LIMIT = boundaries[1];

            for (int WINDOW_SIZE = boundaries[0]; WINDOW_SIZE < WIN_LIMIT; WINDOW_SIZE += boundaries[2]) {
                for (int PAA_SIZE = boundaries[3]; PAA_SIZE < boundaries[4]; PAA_SIZE += boundaries[5]) {
                    // check for invalid cases
                    if (PAA_SIZE > WINDOW_SIZE) {
                        continue;
                    }
                    for (int ALPHABET_SIZE = boundaries[6]; ALPHABET_SIZE < boundaries[7]; ALPHABET_SIZE += boundaries[8]) {
                        paramsList.add(new Tuple3<>(WINDOW_SIZE, PAA_SIZE, ALPHABET_SIZE));
                    }
                }
            }
            // 初始化spark环境
            SparkConf conf = new SparkConf()
                    .setAppName("Spark params sampler written by java")
                    .setMaster("local[*]");
            JavaSparkContext sc = new JavaSparkContext(conf); //其底层就是scala的sparkcontext
            // 并行分区
            JavaRDD<Tuple3<Integer, Integer, Integer>> paramsRDD = sc.parallelize(paramsList);

            JavaPairRDD<SecondSortKey, String> paramsScore = paramsRDD.mapToPair(
                    new PairFunction<Tuple3<Integer, Integer, Integer>,
                            SecondSortKey,
                            String>() {
                        @Override
                        public Tuple2<SecondSortKey, String> call(Tuple3<Integer, Integer, Integer> param)
                                throws Exception {
//                            long res = (long)(param._1() + param._2() + param._3());
//                            String paramString = param._1().toString()+","+param._2().toString()+","+param._3().toString();
//                            SecondSortKey ssk = new SecondSortKey(paramString, res);
//                            return new Tuple2<>(ssk, paramString);
//
//                          // 计算逻辑
                            SampledPoint p = null;
                            System.out.println(param._1() + "  " + param._2() + "  " + param._3());
                            try {
                                p = rp.sample(param._1(), param._2(), param._2(), GIAlgorithm.REPAIR,
                                        RulePrunerParameters.SAX_NR_STRATEGY, RulePrunerParameters.SAX_NORM_THRESHOLD);
                            } catch (InterruptedException e) {
                                System.err.println("Ooops -- was interrupted, finilizing sampling ...");
                            }

                            if (null != p) {
                                String paramString = param._1().toString() + "," + param._2().toString() + "," + param._3().toString() + ","
                                        + p.getApproxDist() + "," + p.getGrammarSize() + "," + p.getGrammarRules() + "," + p.getCompressedGrammarSize()
                                        + "," + p.getPrunedRules() + "," + p.isCovered() + "," + p.getCoverage() + "," + p.getMaxFrequency();
                                SecondSortKey ssk = new SecondSortKey(paramString, (long) p.getReduction());
                                return new Tuple2<SecondSortKey, String>(ssk, paramString);
                            }
                            return new Tuple2<SecondSortKey, String>(new SecondSortKey("", Long.MAX_VALUE), "");
                        }
                    });

            //排序
            JavaPairRDD<SecondSortKey, String> sortByKeyRDD = paramsScore.sortByKey();
            sortByKeyRDD.foreach(new VoidFunction<Tuple2<SecondSortKey, String>>() {
                @Override
                public void call(Tuple2<SecondSortKey, String> s) throws Exception {
                    // TODO Auto-generated method stub
                    System.out.println("sort0:" + "  " + s._1.getSecond() + ", " + s._2);
                }
            });

            //过滤自定义的key
            JavaRDD<String> mapRDD = sortByKeyRDD.map(
                    new Function<Tuple2<SecondSortKey, String>, String>() {
                        @Override
                        public String call(Tuple2<SecondSortKey, String> v1) throws Exception {
                            return v1._1.getSecond() + "\t" + v1._2;
                        }
                    });

            mapRDD.foreach(new VoidFunction<String>() {
                @Override
                public void call(String s) throws Exception {
                    // TODO Auto-generated method stub
                    System.out.println("sort:" + "  " + s);
                }
            });

            String dir = "result";
            FileUtils fileUtils = new FileUtils();
            File file = new File(dir);
            if (file.exists()) {
                if (file.isDirectory()) {
                    try {
                        fileUtils.deleteDirectory(file);
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                } else {
                    file.delete();
                }
            }

            mapRDD.saveAsTextFile(dir);
            sc.close();

            try {
                List<String> list = HdfsOperate.listAll(file.getAbsolutePath());
                for (String remoteFile : list) {//其内部实质上还是调用了迭代器遍历方式，这种循环方式还有其他限制，不建议使用。
                    String remoteFile1 = remoteFile;
                    String fileName = remoteFile1.replaceAll("file:" + file.getAbsolutePath() + "/", "");
                    if (fileName.startsWith("part")) {
                        String result = new String(HdfsOperate.readHDFSFile(remoteFile));
                        System.out.println(result);
                        String[] lines = result.split("\n");
                        for (String line : lines) {
                            if (!line.isEmpty()) {
                                String[] split = line.split("\t");
                                Long score = Long.valueOf(split[0]);
                                String params = split[1];

                                String[] paramList = params.split(",");
                                int WIN = Integer.valueOf(paramList[0]);
                                int PAA = Integer.valueOf(paramList[1]);
                                int ALP = Integer.valueOf(paramList[2]);
                                double approxDist = Double.valueOf(paramList[3]);
                                int grammarSize = Integer.valueOf(paramList[4]);
                                int grammarRulesSize = Integer.valueOf(paramList[5]);
                                int compressedSize = Integer.valueOf(paramList[6]);
                                int prunedRulesSize = Integer.valueOf(paramList[7]);
                                boolean covered = Boolean.valueOf(paramList[8]);
                                double coverage = Double.valueOf(paramList[9]);
                                int maxFreq = Integer.valueOf(paramList[10]);
                                System.out.println("WIN: " + WIN + ", PAA: " + PAA + "，ALP: " + ALP + ", score: " + score);
                                //assertEquals(true, result.length() > 0);
                                SampledPoint p = new SampledPoint();
                                p.setWindow(WIN);
                                p.setPAA(PAA);
                                p.setAlphabet(ALP);
                                p.setApproxDist(approxDist);
                                p.setGrammarSize(grammarSize);
                                p.setGrammarRules(grammarRulesSize);
                                p.setCompressedGrammarSize(compressedSize);
                                p.setPrunedRules(prunedRulesSize);
                                p.setCovered(covered);
                                p.setCoverage(coverage);
                                p.setMaxFrequency(maxFreq);
                                res.add(p);
                            }
                        }
                        LOGGER.info("sampler loop finished " + res.get(0).toString());

                        Collections.sort(res, new ReductionSorter());
                        LOGGER.info("apparently, the best parameters are " + res.get(0).toString());

                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                assertEquals(true, false);
            }

            LOGGER.info("sampler loop finished " + res.get(0).toString());
            Collections.sort(res, new ReductionSorter());
            LOGGER.info("apparently, the best parameters are " + res.get(0).toString());
        }
    }
}
