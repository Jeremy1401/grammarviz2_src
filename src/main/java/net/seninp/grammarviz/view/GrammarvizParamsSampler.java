package net.seninp.grammarviz.view;

import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.seninp.gi.rulepruner.ReductionSorter;
import net.seninp.gi.rulepruner.RulePruner;
import net.seninp.gi.rulepruner.SampledPoint;
import net.seninp.gi.GIAlgorithm;
import net.seninp.gi.rulepruner.RulePrunerParameters;

import spark.core.SecondSortKey;
import spark.core.HdfsOperate;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import static org.junit.Assert.assertEquals;

public class GrammarvizParamsSampler implements Callable<String> {

    private GrammarvizChartPanel parent;

    JavaSparkContext sc;
    boolean useSpark = false;
    // static block - we instantiate the logger
    //
    private static final Logger LOGGER = LoggerFactory.getLogger(GrammarvizParamsSampler.class);

    public GrammarvizParamsSampler(GrammarvizChartPanel grammarvizChartPanel) {
        this.parent = grammarvizChartPanel;
    }

    public void setSc(JavaSparkContext s){
        sc = s;
    }
    public void setUseSpark(boolean flag) {
        useSpark = flag;
    }

    public void cancel() {
        this.parent.actionPerformed(new ActionEvent(this, 0, GrammarvizChartPanel.SELECTION_CANCELLED));
    }

    @Override
    public String call() throws Exception {

        ArrayList<SampledPoint> res = new ArrayList<SampledPoint>();

        this.parent.actionPerformed(new ActionEvent(this, 0, GrammarvizChartPanel.SELECTION_FINISHED));

        double[] ts = Arrays.copyOfRange(this.parent.tsData, this.parent.session.samplingStart,
                this.parent.session.samplingEnd);

        RulePruner rp = new RulePruner(ts);
        int[] boundaries = Arrays.copyOf(this.parent.session.boundaries,
                this.parent.session.boundaries.length);

        //
        //
        LOGGER.info("starting sampling loop on interval [" + this.parent.session.samplingStart + ", "
                + this.parent.session.samplingEnd + "] of length "
                + Integer.valueOf(this.parent.session.samplingEnd - this.parent.session.samplingStart));
        LOGGER.info("window range: " + boundaries[0] + " - " + boundaries[1] + ", step " + boundaries[2]);
        LOGGER.info("PAA range: " + boundaries[3] + " - " + boundaries[4] + ", step " + boundaries[5]);
        LOGGER.info("Alphabet range: " + boundaries[6] + " - " + boundaries[7] + ", step " + boundaries[8]);
        //
        //

        // need to take care about the sliding window size and adjust it
        //
        int samplingIntervalLength = this.parent.session.samplingEnd
                - this.parent.session.samplingStart;
        int WIN_LIMIT = Math.min(samplingIntervalLength, boundaries[1]);

        if (useSpark) {
            // 初始化参数集合
            ArrayList<Tuple3<Integer, Integer, Integer>> paramsList = new ArrayList<>();

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
            System.out.println("here comes");

            // 并行分区
            JavaRDD<Tuple3<Integer, Integer, Integer>> paramsRDD = sc.parallelize(paramsList);

            System.out.println("here comes  1");
            JavaPairRDD<SecondSortKey, String> paramsScore = paramsRDD.mapToPair(
                    new PairFunction<Tuple3<Integer, Integer, Integer>,
                            SecondSortKey,
                            String>() {
                        @Override
                        public Tuple2<SecondSortKey, String> call(Tuple3<Integer, Integer, Integer> param)
                                throws Exception {
                            // 计算逻辑
                            SampledPoint p = null;

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
            System.out.println("here comes  2");
            //排序
            JavaPairRDD<SecondSortKey, String> sortByKeyRDD = paramsScore.sortByKey();
            sortByKeyRDD.foreach(new VoidFunction<Tuple2<SecondSortKey, String>>() {
                @Override
                public void call(Tuple2<SecondSortKey, String> s) throws Exception {
                    // TODO Auto-generated method stub
                    System.out.println("sort0:" + "  " + s._1.getSecond() + ", " + s._2);
                }
            });
            System.out.println("here comes  4");
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
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                assertEquals(true, false);
            }
        } else {

            for (int WINDOW_SIZE = boundaries[0]; WINDOW_SIZE < WIN_LIMIT; WINDOW_SIZE += boundaries[2]) {

                for (int PAA_SIZE = boundaries[3]; PAA_SIZE < boundaries[4]; PAA_SIZE += boundaries[5]) {

                    // System.out.println(WINDOW_SIZE);

                    // check for invalid cases
                    if (PAA_SIZE > WINDOW_SIZE) {
                        continue;
                    }

                    for (int ALPHABET_SIZE = boundaries[6]; ALPHABET_SIZE < boundaries[7]; ALPHABET_SIZE += boundaries[8]) {

                        SampledPoint p = null;

                        try {
                            p = rp.sample(WINDOW_SIZE, PAA_SIZE, ALPHABET_SIZE, GIAlgorithm.REPAIR,
                                    RulePrunerParameters.SAX_NR_STRATEGY, RulePrunerParameters.SAX_NORM_THRESHOLD);
                        } catch (InterruptedException e) {
                            System.err.println("Ooops -- was interrupted, finilizing sampling ...");
                        }

                        if (null != p) {
                            res.add(p);
                        }

                        if (Thread.currentThread().isInterrupted()) {
                            // Cannot use InterruptedException since it's checked
                            System.err.println("Ooops -- was interrupted, finilizing sampling ...");

                            Collections.sort(res, new ReductionSorter());

                            parent.session.saxWindow = res.get(0).getWindow();
                            parent.session.saxPAA = res.get(0).getPAA();
                            parent.session.saxAlphabet = res.get(0).getAlphabet();

                            LOGGER.info("\nApparently, the best parameters are " + res.get(0).toString());
                            this.parent
                                    .actionPerformed(new ActionEvent(this, 0, GrammarvizChartPanel.SAMPLING_SUCCEEDED));

                            return res.get(0).getWindow() + " " + res.get(0).getPAA() + " "
                                    + res.get(0).getAlphabet();

                        }

                    }
                }
            }
        }

        LOGGER.info("sampler loop finished " + res.get(0).toString());

        Collections.sort(res, new ReductionSorter());

        parent.session.saxWindow = res.get(0).getWindow();
        parent.session.saxPAA = res.get(0).getPAA();
        parent.session.saxAlphabet = res.get(0).getAlphabet();

        LOGGER.info("apparently, the best parameters are " + res.get(0).toString());

        this.parent.actionPerformed(new ActionEvent(this, 0, GrammarvizChartPanel.SAMPLING_SUCCEEDED));

        return res.get(0).getWindow() + " " + res.get(0).getPAA() + " " + res.get(0).getAlphabet();
    }

    public void sparkParams(){

    }
}
