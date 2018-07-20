package net.seninp.grammarviz.cluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import hierarchical.Dendrogram;
import model.DataPoint;
import net.seninp.gi.rulepruner.RulePrunerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.beanutils.BeanUtils;
import net.seninp.gi.sequitur.SequiturFactory;
import net.seninp.gi.sequitur.SAXRule;
import net.seninp.gi.logic.GrammarRules;
import net.seninp.gi.logic.GrammarRuleRecord;
import net.seninp.gi.logic.RuleInterval;
import net.seninp.jmotif.sax.SAXProcessor;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;
import net.seninp.jmotif.sax.datastructure.SAXRecords;
import net.seninp.jmotif.sax.NumerosityReductionStrategy;
import net.seninp.jmotif.sax.TSProcessor;
import net.seninp.util.StackTrace;
import rock.ROCKAlgorithm;

public class testCluster {
    private static final String TEST_DATA_FNAME = "C://Users/dsm/Desktop/grammarviz3-optimized/final_1/38.csv";
    private static final String SAVE_DATA_FNAME = "C://Users/dsm/Desktop/grammarviz3-optimized/result_cluster_NONE.txt";

    private static final String LIMIT_STR = "50000";

    final static Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final String SPACE = " ";
    private static final String CR = "\n";

    private static final boolean USE_WINDOW_SLIDE = true;
    private static final int WIN_SIZE = 2000;
    private static final int PAA_SIZE = 10;
    private static final int ALPHABET_SIZE = 4;
    private static final double NORM_THRESHOLD = 0.05;
    private static final int DISCORDS_TO_TEST = 5;
    private static final NumerosityReductionStrategy STRATEGY = NumerosityReductionStrategy.EXACT;

    private double[] series;

    private GrammarRules grammarRules;

    private static final Logger LOGGER = LoggerFactory.getLogger(testCluster.class);

    @Before
    public void setUp() throws Exception {
        // check if everything is ready
        if ((null == this.TEST_DATA_FNAME) || this.TEST_DATA_FNAME.isEmpty()) {
            LOGGER.info("unable to load data - no data source selected yet");
            return;
        }

        // make sure the path exists
        Path path = Paths.get(this.TEST_DATA_FNAME);
        if (!(Files.exists(path))) {
            LOGGER.info("file " + this.TEST_DATA_FNAME + " doesn't exist.");
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
                String[] lineSplit = line.trim().split("/s+");
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
                    + this.TEST_DATA_FNAME + ":\n" + stackTrace);
        } finally {
            assert true;
        }

        // convert to simple doubles array and clean the variable
        if (!(data.isEmpty())) {
            this.series = new double[data.size()];
            for (int i = 0; i < data.size(); i++) {
                this.series[i] = data.get(i);
            }
        }
        data = new ArrayList<Double>();
        LOGGER.info("loaded " + this.series.length + " points....");
    }

    @Test
    public void test() {
        // check if the data is loaded
        //
        if (null == this.series || this.series.length == 0) {
            LOGGER.info("unable to \"Process data\" - no data were loaded ...");
        } else {

            // the logging block
            //
            StringBuffer sb = new StringBuffer("setting up GI with params: ");
            sb.append("sliding window ").append(USE_WINDOW_SLIDE);
            sb.append(", numerosity reduction ").append(STRATEGY.toString());
            sb.append(", SAX window ").append(WIN_SIZE);
            sb.append(", PAA ").append(PAA_SIZE);
            sb.append(", Alphabet ").append(ALPHABET_SIZE);
            LOGGER.info(sb.toString());

            NormalAlphabet na = new NormalAlphabet();

            try {

                SAXProcessor sp = new SAXProcessor();

                SAXRecords saxFrequencyData = new SAXRecords();
                if (USE_WINDOW_SLIDE) {
                    saxFrequencyData = sp.ts2saxViaWindow(series, WIN_SIZE, PAA_SIZE,
                            na.getCuts(ALPHABET_SIZE),
                            STRATEGY, NORM_THRESHOLD);  // reviewed
//                    LOGGER.info(saxFrequencyData.getIndexes().toString());
//                    LOGGER.info(saxFrequencyData.getSAXString(SPACE));
//
//                    ArrayList<Integer> keys = saxFrequencyData.getAllIndices();
//                    for (int i : keys) {
//                        LOGGER.info(i + "," + String.valueOf(saxFrequencyData.getByIndex(i).getPayload()));
//                    }
                } else {
                    saxFrequencyData = sp.ts2saxByChunking(series, PAA_SIZE, na.getCuts(ALPHABET_SIZE),
                            NORM_THRESHOLD);
                }

                SAXRule sequiturGrammar = SequiturFactory
                        .runSequitur(saxFrequencyData.getSAXString(SPACE));

                grammarRules = sequiturGrammar.toGrammarRulesData();

                SequiturFactory.updateRuleIntervals(grammarRules, saxFrequencyData, USE_WINDOW_SLIDE,
                        series, WIN_SIZE, PAA_SIZE);

                // prune rules
                GrammarRules prunedRulesSet = RulePrunerFactory.performPruning(series,
                        grammarRules);
                grammarRules = prunedRulesSet;

                //Define data
                DataPoint[] elements = new DataPoint[grammarRules.size()];
                int count = 0;
                for (GrammarRuleRecord rule : grammarRules) {
                    LOGGER.info("rule Number: " + rule.getRuleNumber());
                    elements[count++] = new DataPoint(rule.getRuleName(), new String[] {rule.getExpandedRuleString()});
                }

                int k = 1;
                double th = 0.6;
                ROCKAlgorithm rock = new ROCKAlgorithm(elements, k, th);
                rock.getLinkMatrix().printPointLinkMatrix();
                rock.getLinkMatrix().printSimilarityMatrix();
                rock.getLinkMatrix().printPointNeighborMatrix();
                Dendrogram dnd = rock.cluster();
                dnd.printAll();
                LOGGER.info("size: " + grammarRules.size());

                if(true){
                    return;
                }
                // show the rule Interval
//                for (GrammarRuleRecord ruleRecord : grammarRules) {
//                    LOGGER.info(ruleRecord.getRuleName() + " : ");
//                    for (RuleInterval ruleInterval : ruleRecord.getRuleIntervals()) {
//                        LOGGER.info(ruleInterval.getId() + " " + ruleInterval.toString());
//                    }
//                }

//                RuleClusters clusters = RuleClusterFactory.runRuleCluster(grammarRules);
//                LOGGER.info("共有" + clusters.getK() + "规则簇");
//                int i = 0;
//                for (RuleCluster cluster : clusters.getRuleClusters()) {
//                    i++;
//                    LOGGER.info("第" + i + "个簇： 簇心： " + cluster.getCenterId() + " ，包含 "
//                            + cluster.getRuleIds().toString()
//                            + " 距簇心的距离和" + cluster.getDistanceSum());
//                }

                // get the distance database
                HashMap<Integer, Map<Integer, Double>> distanceDatabase = RuleClusterFactory.getDistanceDatabase(grammarRules);

//
//                for(Integer ruleId1 : distanceDatabase.keySet()){
//                    for(Integer ruleId2 : distanceDatabase.get(ruleId1).keySet()){
//                        LOGGER.info(grammarRules.getRuleRecord(ruleId1).getRuleName() + " - "
//                                + grammarRules.getRuleRecord(ruleId2).getRuleName() + " : "
//                                + distanceDatabase.get(ruleId1).get(ruleId2));
//                    }
//                }
                if(true){
                    return;
                }
                Map<Integer, Map<Double, ArrayList<Integer>>> distanceMap = new TreeMap<>();

                for (Integer key : distanceDatabase.keySet()) {
                    double dis = 0.0D;
                    Map<Double, ArrayList<Integer>> map = new TreeMap<>();
                    for (Integer subKey : distanceDatabase.get(key).keySet()) {
                        dis = distanceDatabase.get(key).get(subKey);
                        if (!map.containsKey(dis)) {
                            ArrayList<Integer> keyArray = new ArrayList<>();
                            keyArray.add(subKey);
                            map.put(dis, keyArray);
                        } else {
                            map.get(dis).add(subKey);
                        }
                    }
                    distanceMap.put(key, map);
                }

                for (Map.Entry<Integer, Map<Double, ArrayList<Integer>>> entry : distanceMap.entrySet()) {
                    LOGGER.info("与R" + entry.getKey() + " 相距 ");
                    for (Double dis : entry.getValue().keySet()) {
                        if(dis > 3){
                            break;
                        }
                        LOGGER.info(dis + " 的有：" + entry.getValue().get(dis).toString());
                    }
                }

                LOGGER.info("规则个数: " + grammarRules.size());

                RuleClusters ruleClusters = new RuleClusters();
                ArrayList<Integer> usedRuleIdList = new ArrayList<>(grammarRules.size());
//                ArrayList<Integer> random = new ArrayList<>();
//                for (int id = 1; id < grammarRules.size(); id++) {
//                    random.add(id);
//                }
//                Collections.shuffle(random);
//                LOGGER.info("乱序之后： " + random.toString());
//
//                for (Integer ruleId : random) {
//                    LOGGER.info("now ruleId = " + ruleId);
//                    RuleCluster ruleCluster = new RuleCluster();
//                    ruleCluster.setCenterId(ruleId);
//                    ruleCluster.addRuleIntervals(grammarRules.get(ruleId).getRuleIntervals());
//                    if (usedRuleIdList.contains(ruleId)) {
//                        continue;
//                    }
//                    usedRuleIdList.add(ruleId);
//                    // LOGGER.info(distanceMap.get(ruleId).keySet().toString());
//                    for (Double dis : distanceMap.get(ruleId).keySet()) {
//                        if (dis > 3) {
//                            break;
//                        }
//                        //LOGGER.info("entry.getValue().get(dis): " + entry.getValue().get(dis).toString());
//                        for (Integer id : distanceMap.get(ruleId).get(dis)) {
//                            if (usedRuleIdList.contains(id)) {
//                                continue;
//                            }
//                            usedRuleIdList.add(id);
//                            ruleCluster.addRuleId(id);
//                            ruleCluster.addRuleIntervals(grammarRules.get(id).getRuleIntervals());
//                        }
//                    }
//                    ruleCluster.reorganizeRuleIntervals();
//                    ruleCluster.calculateDistanceSum(distanceDatabase);
//                    ruleClusters.addRuleCluster(ruleCluster);
//                }
//                ruleClusters.calculateAggregation();
//                ruleClusters.calculateDispersion(distanceDatabase);
//                LOGGER.info("共有 " + ruleClusters.getK() + "个簇, 聚合度： "
//                        + ruleClusters.getAggregation() + ", 离散度"
//                        + ruleClusters.getDispersion());
//                LOGGER.info("具体信息如下：");
//                for (RuleCluster ruleCluster : ruleClusters.getRuleClusters()) {
//                    LOGGER.info("具体信息如下：" + ruleCluster.toString());
//                }
//
//                LOGGER.info("乱序前：");
//                usedRuleIdList.clear();
                ruleClusters = new RuleClusters();
                for (Map.Entry<Integer, Map<Double, ArrayList<Integer>>> entry : distanceMap.entrySet()) {
                    RuleCluster ruleCluster = new RuleCluster();
                    ruleCluster.setCenterId(entry.getKey());
                    ruleCluster.addRuleIntervals(grammarRules.get(entry.getKey()).getRuleIntervals());
                    if (usedRuleIdList.contains(entry.getKey())) {
                        continue;
                    }
                    usedRuleIdList.add(entry.getKey());
                    for (Double dis : entry.getValue().keySet()) {
                        if (dis > 3) {
                            break;
                        }
                        //LOGGER.info("entry.getValue().get(dis): " + entry.getValue().get(dis).toString());
                        for (Integer id : entry.getValue().get(dis)) {
                            if (usedRuleIdList.contains(id)) {
                                continue;
                            }
                            usedRuleIdList.add(id);
                            ruleCluster.addRuleId(id);
                            ruleCluster.addRuleIntervals(grammarRules.get(id).getRuleIntervals());
                        }
                    }
                    ruleCluster.reorganizeRuleIntervals();
                    ruleCluster.calculateDistanceSum(distanceDatabase);
                    ruleClusters.addRuleCluster(ruleCluster);
                }

                ruleClusters.calculateAggregation();
                ruleClusters.calculateDispersion(distanceDatabase);

                LOGGER.info("共有 " + ruleClusters.getK() + "个簇, 聚合度： "
                        + ruleClusters.getAggregation() + ", 离散度"
                        + ruleClusters.getDispersion());
                LOGGER.info("具体信息如下：");
                for (RuleCluster ruleCluster : ruleClusters.getRuleClusters()) {
                    LOGGER.info("具体信息如下：" + ruleCluster.toString());
                }
                if (true) {
                    return;
                }

            } catch (Exception e) {
                LOGGER.info("error while processing data " + StackTrace.toString(e));
                e.printStackTrace();
            }

            LOGGER.info("processed data, broadcasting charts");
            LOGGER.info("process finished");

            // find the top 5 anomalies by HOTSAX
//            int[] discordRuleIndex = new int[DISCORDS_TO_TEST];
//            int index = 0;
//            DiscordRecords discordsHash = null;
//            try {
//                discordsHash = HOTSAXImplementation.series2Discords(series, DISCORDS_TO_TEST, WIN_SIZE,
//                        PAA_SIZE, ALPHABET_SIZE, STRATEGY, NORM_THRESHOLD);
//                for (DiscordRecord d : discordsHash) {
//                    discordRuleIndex[index] = d.getPosition();
//                    LOGGER.info("hotsax hash discord " + d.toString() + " length: " + d.getLength() + " ruleId: " + d.getRuleId());
//                    index++;
//                }
//            } catch (Exception e) {
//                fail("shouldn't throw an exception, exception thrown: \n" + StackTrace.toString(e));
//                e.printStackTrace();
//            }

            // use k-means to cluster the rules pruned the discord

        }
    }

    @Test
    public void testStringSub() {
        char a = 'a';
        char b = 'z';
        LOGGER.info("a-b= " + Math.abs(a - b));
    }

    @After
    public void saveGrammarRules() {
        if (true) {
            return;
        }
        List exportData = new ArrayList<Map>();
        LinkedHashMap map = new LinkedHashMap();
        //设置列名
        map.put("1", "Grammar Rule");
        map.put("2", "Expanded Rule");
        map.put("3", "Index");
        map.put("4", "SubSequence Starts");
        map.put("5", "SubSequence lengths");
        map.put("6", "Occurrence Frequency");
        map.put("7", "Use Frequency");
        map.put("8", "Min Length");
        map.put("9", "Max length");
        map.put("10", "Mean length");

        for (GrammarRuleRecord ruleRecord : grammarRules) {
            Map row = new LinkedHashMap<Integer, Double>();

            row.put("1", ruleRecord.getRuleString());
            row.put("2", ruleRecord.getExpandedRuleString());
            row.put("3", ruleRecord.getRuleName());

            if (ruleRecord.getRuleIntervals().size() > 0) {

                int[] starts = new int[ruleRecord.getRuleIntervals().size()];
                int[] lengths = new int[ruleRecord.getRuleIntervals().size()];
                int i = 0;
                for (RuleInterval sp : ruleRecord.getRuleIntervals()) {
                    starts[i] = sp.getStart();
                    lengths[i] = (sp.endPos - sp.startPos);
                    i++;
                }
                row.put("4", toString(starts));
                row.put("5", toString(lengths));
            }

            row.put("6", ruleRecord.getRuleIntervals().size());
            row.put("7", ruleRecord.getRuleUseFrequency());
            row.put("8", ruleRecord.minMaxLengthAsString().split(" - ")[0]);
            row.put("9", ruleRecord.minMaxLengthAsString().split(" - ")[1]);
            row.put("10", ruleRecord.getMeanLength());
            exportData.add(row);
        }

        //这个文件上传到路径，可以配置在数据库从数据库读取，这样方便一些！
        String path = "/Users/Jeremy/Desktop/毕设/参考文献/导师推荐/代码/grammarviz3/";

        //文件名=生产的文件名称+时间戳
        String fileName = "文件导出";
        File file = createCSVFile(exportData, map, path, fileName);
        String fileName2 = file.getName();
        LOGGER.info("文件名称：" + fileName2);

        TSProcessor tsProcessor = new TSProcessor();
        double[] normalizedData = tsProcessor.znorm(series, NORM_THRESHOLD);

        // 保存z-normalized之后的时间序列
        boolean fileOpen = false;
        BufferedWriter bw = null;
        StringBuffer sb = new StringBuffer();
        try {
            String currentPath = new File(".").getCanonicalPath();
            bw = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(currentPath + File.separator + "normalized.txt"), "UTF-8"));
            fileOpen = true;
            for (int i = 0; i < normalizedData.length; i++) {
                sb.append(normalizedData[i]).append("\n");
            }

            bw.write(sb.toString());
        } catch (IOException e) {
            System.err.print(
                    "Encountered an error while writing stats file: \n" + StackTrace.toString(e) + "\n");
        }

        if (fileOpen) {
            try {
                bw.write(sb.toString());
            } catch (IOException e) {
                System.err.print(
                        "Encountered an error while writing stats file: \n" + StackTrace.toString(e) + "\n");
            }
        }
    }

    public static File createCSVFile(List exportData, LinkedHashMap map, String outPutPath,
                                     String fileName) {
        File csvFile = null;
        BufferedWriter csvFileOutputStream = null;
        try {
            File file = new File(outPutPath);
            if (!file.exists()) {
                file.mkdir();
            }
            //定义文件名格式并创建
            csvFile = File.createTempFile(fileName, ".csv", new File(outPutPath));
            LOGGER.info("csvFile：" + csvFile);
            // UTF-8使正确读取分隔符","
            //如果生产文件乱码，windows下用gbk，linux用UTF-8
            csvFileOutputStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                    csvFile), "UTF-8"), 1024);
            LOGGER.info("csvFileOutputStream：" + csvFileOutputStream);
            // 写入文件头部
            for (Iterator propertyIterator = map.entrySet().iterator(); propertyIterator.hasNext(); ) {
                java.util.Map.Entry propertyEntry = (java.util.Map.Entry) propertyIterator.next();
                csvFileOutputStream.write((String) propertyEntry.getValue() != null ? (String) propertyEntry.getValue() : "");
                if (propertyIterator.hasNext()) {
                    csvFileOutputStream.write(",");
                }
            }
            csvFileOutputStream.newLine();
            // 写入文件内容
            for (Iterator iterator = exportData.iterator(); iterator.hasNext(); ) {
                Object row = (Object) iterator.next();
                for (Iterator propertyIterator = map.entrySet().iterator(); propertyIterator
                        .hasNext(); ) {
                    java.util.Map.Entry propertyEntry = (java.util.Map.Entry) propertyIterator
                            .next();
                    csvFileOutputStream.write((String) BeanUtils.getProperty(row,
                            (String) propertyEntry.getKey()));
                    if (propertyIterator.hasNext()) {
                        csvFileOutputStream.write(",");
                    }
                }
                if (iterator.hasNext()) {
                    csvFileOutputStream.newLine();
                }
            }
            csvFileOutputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                csvFileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return csvFile;
    }


    public void testCreateCSVFile() {
        List exportData = new ArrayList<Map>();
        Map row1 = new LinkedHashMap<Integer, Double>();
        row1.put("1", "11");
        row1.put("2", "12");
        row1.put("3", "13");
        row1.put("4", "14");
        exportData.add(row1);
        row1 = new LinkedHashMap<Integer, Double>();
        row1.put("1", "21");
        row1.put("2", "22");
        row1.put("3", "23");
        row1.put("4", "24");
        exportData.add(row1);
        LinkedHashMap map = new LinkedHashMap();
        //设置列名
        map.put("1", "Grammar");
        map.put("2", "Index");
        map.put("3", "Frequency");
        map.put("4", "Use");
        //这个文件上传到路径，可以配置在数据库从数据库读取，这样方便一些！
        String path = "/Users/Jeremy/Desktop/毕设/参考文献/导师推荐/代码/grammarviz3/";

        //文件名=生产的文件名称+时间戳
        String fileName = "文件导出";
        File file = createCSVFile(exportData, map, path, fileName);
        String fileName2 = file.getName();
        LOGGER.info("文件名称：" + fileName2);
    }

    public static String toString(int[] a) {
        if (a == null)
            return "null";
        int iMax = a.length - 1;
        if (iMax == -1)
            return "[]";

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            b.append(a[i]);
            if (i == iMax)
                return b.append(']').toString();
            b.append("- ");
        }
    }
}
