package net.seninp.grammarviz.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RuleClusters {
    private int k;  // the number of clusters
    private ArrayList<RuleCluster> ruleClusters = new ArrayList<>();
    private double dispersion;  // the sum of distances between minTree cluster center id
    private double aggregation;  // the sum of all distanceSum / k
    // the logger
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleClusters.class);

    public RuleClusters() {
        k = 0;
        ruleClusters = new ArrayList<RuleCluster>();
        dispersion = 0;
        aggregation = Double.MAX_VALUE;
    }

    public RuleClusters(int k, ArrayList<RuleCluster> ruleClusters, double dispersion, double aggregation) {
        this.k = k;
        this.ruleClusters = ruleClusters;
        this.dispersion = dispersion;
        this.aggregation = aggregation;
    }

    public RuleClusters(RuleCluster originRules) {
        k = 1;
        ruleClusters.add(originRules);
        dispersion = 0;
        aggregation = originRules.getDistanceSum();
    }

    public RuleClusters(ArrayList<Integer> originRulesIdList) {
        this.k = 1;
        RuleCluster cluster = new RuleCluster(originRulesIdList);
        this.ruleClusters.add(cluster);
        this.dispersion = 0;
        this.aggregation = Double.MAX_VALUE;
    }

    public void reset() {
        k = 0;
        ruleClusters = new ArrayList<RuleCluster>();
        dispersion = Integer.MAX_VALUE;
        aggregation = 0;
    }

    public void setK(int k) {
        this.k = k;
    }

    public int getK() {
        return this.k;
    }

    public void setRuleClusters(ArrayList<RuleCluster> ruleClusters) {
        this.ruleClusters = ruleClusters;
    }

    public ArrayList<RuleCluster> getRuleClusters() {
        return this.ruleClusters;
    }

    public RuleCluster getRuleClusterByIndex(int index) {
        return this.ruleClusters.get(index);
    }

    public void addRuleClusters(ArrayList<RuleCluster> ruleClusters) {
        this.ruleClusters.addAll(ruleClusters);
        this.k += ruleClusters.size();
    }

    public void addRuleCluster(RuleCluster ruleCluster) {
        this.ruleClusters.add(ruleCluster);
        this.k += 1;
    }

    public void setDispersion(double dispersion) {
        this.dispersion = dispersion;
    }

    public double getDispersion() {
        return this.dispersion;
    }

    public void calculateDispersion(HashMap<Integer, Map<Integer, Double>> distanceDatabase) {
        if(k < 2){
            return;
        }
        LOGGER.info("计算簇的离散度(共"+k+"个簇)：");
        ArrayList<Integer> centerIdList = new ArrayList<>();
        for (RuleCluster cluster : this.ruleClusters) {
            int centerId = cluster.getCenterId();
            centerIdList.add(centerId);
        }
        // get the graph
        double[][] graph = new double[k][k];
        for (int i = 0; i < k - 1; i++) {
            for (int j = i + 1; j < k; j++) {
                int id1 = centerIdList.get(i);
                int id2 = centerIdList.get(j);
                graph[i][j] = graph[j][i] = distanceDatabase.get(id1).get(id2);
            }
        }

//        for(int i = 0; i<k;i++){
//            for(int j=0;j<k;j++){
//                int id1 = centerIdList.get(i);
//                int id2 = centerIdList.get(j);
//                LOGGER.info(id1 + " - " + id2 + " : " + graph[i][j] + "\t");
//            }
//            LOGGER.info("\n");
//        }
        // get the min tree by prim
        double[] lowCost = new double[k];  //到新集合的最小权
        int[] mid = new int[k]; //存取前驱结点
        ArrayList<Integer> minTreeList = new ArrayList<>();//用来存储加入结点的顺序
        int i, j, minId = 0;
        double min, sum = 0.0D;

        //初始化辅助数组
        for (i = 1; i < k; i++) {
            lowCost[i] = graph[0][i];
            mid[i] = 0;
        }
        minTreeList.add(centerIdList.get(0));
        //一共需要加入k-1个点
        for (i = 1; i < k; i++) {
            min = Double.MAX_VALUE;
            minId = 0;
            //每次找到距离集合最近的点
            for (j = 1; j < k; j++) {
                if (lowCost[j] != 0 && lowCost[j] < min) {
                    min = lowCost[j];
                    minId = j;
                }
            }

            if (minId == 0) return;
            minTreeList.add(centerIdList.get(minId));
            lowCost[minId] = 0;
            sum += min;
            //LOGGER.info(centerIdList.get(mid[minId]) + "到" + centerIdList.get(minId) + " 权值：" + min);
            //加入该点后，更新其它点到集合的距离
            for (j = 1; j < k; j++) {
                if (lowCost[j] != 0 && lowCost[j] > graph[minId][j]) {
                    lowCost[j] = graph[minId][j];
                    mid[j] = minId;
                }
            }
        }
        LOGGER.info("sum:" + sum);
        this.dispersion = sum / this.k;
    }

    public void setAggregation(double aggregation) {
        this.aggregation = aggregation;
    }

    public double getAggregation() {
        return this.aggregation;
    }

    public void calculateAggregation() {
        for (RuleCluster ruleCluster : ruleClusters) {
            this.aggregation += ruleCluster.getDistanceSum();
        }
        this.aggregation /= k;
    }

    public RuleClusters clone() {
        RuleClusters retRuleClusters = new RuleClusters();
        retRuleClusters.setK(this.k);
        retRuleClusters.setRuleClusters(this.ruleClusters);
        retRuleClusters.setAggregation(this.aggregation);
        retRuleClusters.setDispersion(this.dispersion);
        return retRuleClusters;
    }

}
