package cluster.kmeans.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.util.DistanceFunction;
import com.util.DistanceFunctionFactory;
import net.seninp.gi.logic.GrammarRuleRecord;

/**
 * K均值聚类算法
 */
public class KMeansClustering {
    private static final Log LOGGER = LogFactory.getLog(KMeansClustering.class);
    private int k; // 分成多少簇
    private int m; // 迭代次数
    private int dataSetLength; // 数据集元素个数，即数据集的长度
    private List<GrammarRuleRecord> dataSet; // 数据集链表
    private List<GrammarRuleRecord> center; // 中心链表
    private List<List<GrammarRuleRecord>> cluster; // 簇
    private List<Float> jc; // 误差平方和，k越接近dataSetLength，误差越小
    private Random random;

//    public static void main(String[] args) {
//        // 初始化一个Kmean对象，将k置为10
//        KMeansClustering k = new KMeansClustering(5);
//        // 初始化试验集
//        ArrayList<GrammarRuleRecord> dataSet = new ArrayList<GrammarRuleRecord>();
//
//        dataSet.add(new GrammarRuleRecord{1, 2});
//        dataSet.add(new GrammarRuleRecord{3, 3});
//        dataSet.add(new GrammarRuleRecord{3, 4});
//        dataSet.add(new GrammarRuleRecord{5, 6});
//        dataSet.add(new GrammarRuleRecord{8, 9});
//        dataSet.add(new GrammarRuleRecord{4, 5});
//        dataSet.add(new GrammarRuleRecord{6, 4});
//        dataSet.add(new GrammarRuleRecord{3, 9});
//        dataSet.add(new GrammarRuleRecord{5, 9});
//        dataSet.add(new GrammarRuleRecord{4, 2});
//        dataSet.add(new GrammarRuleRecord{1, 9});
//        dataSet.add(new GrammarRuleRecord{7, 8});
//        // 设置原始数据集
//        k.setDataSet(dataSet);
//        // 执行算法
//        k.execute();
//        // 得到聚类结果
//        List<List<GrammarRuleRecord>> cluster = k.getCluster();
//        // 查看结果
//        for (int i = 0; i < cluster.size(); i++) {
//            //CommonUtil.printDataArray(cluster.get(i), "cluster[" + i + "]");
//        }
//
//    }

    /**
     * 设置需分组的原始数据集
     *
     * @param dataSet
     */

    public void setDataSet(List<GrammarRuleRecord> dataSet) {
        this.dataSet = dataSet;
    }

    /**
     * 获取结果分组
     *
     * @return 结果集
     */

    public List<List<GrammarRuleRecord>> getCluster() {
        return cluster;
    }

    /**
     * 构造函数，传入需要分成的簇数量
     *
     * @param k 簇数量,若k<=0时，设置为1，若k大于数据源的长度时，置为数据源的长度
     */
    public KMeansClustering(int k) {
        if (k <= 0) {
            k = 1;
        }
        this.k = k;
    }

    /**
     * 初始化
     */
    private void init() {
        m = 0;
        random = new Random();
        if (dataSet == null || dataSet.size() == 0) {
            LOGGER.info("error: dataSet is null!");
            return;
        }
        dataSetLength = dataSet.size();
        if (k > dataSetLength) {
            k = dataSetLength;
        }
        center = initCenters();
        cluster = initCluster();
        jc = new ArrayList<Float>();
    }

    /**
     * 初始化中心数据链表，分成多少簇就有多少个中心点
     *
     * @return 中心点集
     */
    private ArrayList<GrammarRuleRecord> initCenters() {
        ArrayList<GrammarRuleRecord> center = new ArrayList<GrammarRuleRecord>();
        int[] randoms = new int[k];
        boolean flag;
        int temp = random.nextInt(dataSetLength);
        randoms[0] = temp;
        for (int i = 1; i < k; i++) {
            flag = true;
            while (flag) {
                temp = random.nextInt(dataSetLength);
                int j = 0;

                while (j < i) {
                    if (temp == randoms[j]) {
                        break;
                    }
                    j++;
                }
                if (j == i) {
                    flag = false;
                }
            }
            randoms[i] = temp;
        }

        for (int i = 0; i < k; i++) {
            center.add(dataSet.get(randoms[i])); // 生成初始化中心链表
        }
        return center;
    }

    /**
     * 初始化簇集合
     *
     * @return 一个分为k簇的空数据的簇集合
     */
    private List<List<GrammarRuleRecord>> initCluster() {
        List<List<GrammarRuleRecord>> cluster = new ArrayList();
        for (int i = 0; i < k; i++) {
            cluster.add(new ArrayList<GrammarRuleRecord>());
        }

        return cluster;
    }

    /**
     * 获取距离集合中最小距离的位置
     *
     * @param distance 距离数组
     * @return 最小距离在距离数组中的位置
     */
    private int minDistance(float[] distance) {
        float minDistance = distance[0];
        int minLocation = 0;
        for (int i = 1; i < distance.length; i++) {
            if (distance[i] < minDistance) {
                minDistance = distance[i];
                minLocation = i;
            } else if (distance[i] == minDistance) // 如果相等，随机返回一个位置
            {
                if (random.nextInt(10) < 5) {
                    minLocation = i;
                }
            }
        }

        return minLocation;
    }

    /**
     * 核心，将当前元素放到最小距离中心相关的簇中
     */
    private void clusterSet() {
        float[] distance = new float[k];
        final DistanceFunction distFn;
        distFn = DistanceFunctionFactory.getDistFnByName("EditDistance");
        for (int i = 0; i < dataSetLength; i++) {
            for (int j = 0; j < k; j++) {
                //distance[j] = com.dtw.DTW.getWarpInfoBetween(dataSet.get(i), center.get(j), distFn);
            }
            int minLocation = minDistance(distance);

            cluster.get(minLocation).add(dataSet.get(i));// 核心，将当前元素放到最小距离中心相关的簇中

        }
    }

    /**
     * 计算误差平方和准则函数方法
     */
    private void countRule() {
        float jcF = 0;
        for (int i = 0; i < cluster.size(); i++) {
            for (int j = 0; j < cluster.get(i).size(); j++) {
                //jcF += CommonUtil.errorSquare(cluster.get(i).get(j), center.get(i));
            }
        }
        jc.add(jcF);
    }

    /**
     * 设置新的簇中心方法
     */
    private void setNewCenter() {
        for (int i = 0; i < k; i++) {
            int n = cluster.get(i).size();
            if (n != 0) {
                GrammarRuleRecord newCenter = cluster.get(i).get(0);
                for (int j = 0; j < n; j++) {
//                    newCenter.
                }
                // 设置一个平均值
//                newCenter[0] = newCenter[0] / n;
//                newCenter[1] = newCenter[1] / n;
                center.set(i, newCenter);
            }
        }
    }

    public List<GrammarRuleRecord> getCenter() {
        return center;
    }

    public void setCenter(List<GrammarRuleRecord> center) {
        this.center = center;
    }


    /**
     * Kmeans算法核心过程方法
     */
    private void kmeans() {
        init();

        // 循环分组，直到误差不变为止
        while (true) {
            clusterSet();
            countRule();

            if (m != 0) {
                if (jc.get(m) - jc.get(m - 1) == 0) {
                    break;
                }
            }

            setNewCenter();

            m++;
            cluster.clear();
            cluster = initCluster();
        }

    }

    /**
     * 执行算法
     */
    public void execute() {
        long startTime = System.currentTimeMillis();
        System.out.println("kmeans begins");
        kmeans();
        long endTime = System.currentTimeMillis();
        System.out.println("kmeans running time=" + (endTime - startTime)
                + "ms");
        System.out.println("kmeans ends");
        System.out.println();
    }
}