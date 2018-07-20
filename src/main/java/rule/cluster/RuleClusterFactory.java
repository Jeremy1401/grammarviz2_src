package rule.cluster;

import com.RuleDTW;
import net.seninp.gi.logic.GrammarRuleRecord;
import net.seninp.gi.logic.GrammarRules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class RuleClusterFactory {

    private static double elevator = 0.0;  // save the previous the elevator of cluster result

    // the logger
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleClusterFactory.class);

    /**
     * Disabling the constructor.
     */
    private RuleClusterFactory() {
        assert true;
    }

    public static RuleClusters runRuleCluster(GrammarRules grammarRules) throws Exception {
        // get the distance database
        HashMap<Integer, Map<Integer, Double>> distanceDatabase = getDistanceDatabase(grammarRules);

        // reset the clusters,
        // at first, there is only one rule cluster.
        ArrayList<Integer> ruleIdList = new ArrayList<>();
        for (Integer id : distanceDatabase.keySet()) {
            ruleIdList.add(id);
        }

        // initial the clusters ---- k = 1
        RuleCluster ruleClusterFirst = new RuleCluster(ruleIdList);

        // get the cluster center
        ArrayList<Integer> clusterCenterList = getNewCenterRuleId(distanceDatabase, ruleClusterFirst);

        // set the first rule cluster
        int centerId = clusterCenterList.get(0);
        ruleClusterFirst.setCenterId(centerId);
        // set the distance sum
        ruleClusterFirst.calculateDistanceSum(distanceDatabase);

        // set the clusters
        RuleClusters retClusters = new RuleClusters(ruleClusterFirst);

        LOGGER.info("初始簇个数：" + retClusters.getK()
                + " 规则：" + retClusters.getRuleClusterByIndex(0).getRuleIds().toString()
                + " 初始簇心：" + retClusters.getRuleClusterByIndex(0).getCenterId()
                + " 聚合度： " + retClusters.getAggregation()
                + " 离散度：" + retClusters.getDispersion()
                + "\n");

        int i = 0;
        int maxI = (int)Math.floor(Math.sqrt(ruleIdList.size()));
        LOGGER.info("max = " + maxI);
        // cluster the rules
        while (true) {
            // save the clusters
            RuleClusters lastClusters = retClusters.clone();
            LOGGER.info("保存簇：k = " + lastClusters.getK()
                    + " 簇的个数：" + lastClusters.getRuleClusters().size()
                    + " 聚合度： " + lastClusters.getAggregation()
                    + " 离散度：" + lastClusters.getDispersion()
                    + "\n");
            /*
            int n = 0;
            for (RuleCluster ruleCluster : lastClusters.getRuleClusters()) {
             LOGGER.info("第 " + n + "个簇"
             + " 簇心为： " + ruleCluster.getCenterId()
             + " 规则个数：" + ruleCluster.getRuleIds().size()
             + " 具体规则为：" + ruleCluster.getRuleIds().toString()
             + " 聚合度：" + ruleCluster.getDistanceSum());
             n++;
             }
             LOGGER.info("\n");
             */

            // reset the clusters
            retClusters.reset();

            // update the clusters
            for (RuleCluster ruleCluster : lastClusters.getRuleClusters()) {
                LOGGER.info("现在对以 R" + ruleCluster.getCenterId() + " 为中心的簇进行处理...");
                ArrayList<RuleCluster> newRuleClusterList = ruleClusterBisecting(distanceDatabase, ruleCluster);
                retClusters.addRuleClusters(newRuleClusterList);
//                if (newRuleClusterList.size() == 1) {
//                    LOGGER.info("以R" + newRuleClusterList.get(0).getCenterId() + "为中心的簇未发生分裂");
//                }
//                else{
//                    LOGGER.info("以R" + ruleCluster.getCenterId() + "为中心的簇分裂，新的簇心为R"
//                            + newRuleClusterList.get(0).getCenterId() + " 和 R"
//                            + newRuleClusterList.get(1).getCenterId() + "\n");
//                    LOGGER.info("目前共"+retClusters.getK()+"个簇\n");
//                }
                LOGGER.info("----------------处理结束--------------\n");
            }
            // calculate the dispersion and aggregation of the clusters
            retClusters.calculateDispersion(distanceDatabase);
            retClusters.calculateAggregation();

            if(retClusters.getK() == lastClusters.getK()){
                break;
            }
            // judge whether the end condition satisfies or not
            if (!toSplit(lastClusters, retClusters) && i != 0 || retClusters.getK() >= maxI) {
                retClusters = lastClusters.clone();
                break;
            }
            i++;
        }

        LOGGER.info("\n共进行"+i+"轮循环");
        return retClusters;
    }

    /**
     * update rule clusters
     *
     * @param distanceDatabase
     * @param ruleCluster
     * @return
     */
    public static ArrayList<RuleCluster> ruleClusterBisecting(HashMap<Integer, Map<Integer, Double>> distanceDatabase,
                                                              RuleCluster ruleCluster) {

        LOGGER.info("进行二分操作：");
        ArrayList<RuleCluster> retRuleClusterList = new ArrayList<>();
        if(ruleCluster.getRuleIds().size() < 2){
            retRuleClusterList.add(ruleCluster);
            return retRuleClusterList;
        }
        ArrayList<Integer> clusterCenterList = getNewCenterRuleId(distanceDatabase, ruleCluster);
        int centerRuleId1 = clusterCenterList.get(0);
        int centerRuleId2 = clusterCenterList.get(1);
        LOGGER.info("新的簇心为：R" + centerRuleId1 + " 和 R" + centerRuleId2);
        RuleCluster ruleCluster1 = new RuleCluster();
        RuleCluster ruleCluster2 = new RuleCluster();
        ruleCluster1.setCenterId(centerRuleId1);
        ruleCluster2.setCenterId(centerRuleId2);

        // get new rule clusters
        for (Integer ruleId : ruleCluster.getRuleIds()) {
            if(ruleId == centerRuleId1 || ruleId == centerRuleId2){
                continue;
            }
            double distance1 = distanceDatabase.get(ruleId).get(centerRuleId1);
            double distance2 = distanceDatabase.get(ruleId).get(centerRuleId2);
            if (distance1 < distance2) {
                ruleCluster1.addRuleId(ruleId);
            } else {
                ruleCluster2.addRuleId(ruleId);
            }
        }

        // calculate the sum of distance
        ruleCluster1.calculateDistanceSum(distanceDatabase);
        ruleCluster2.calculateDistanceSum(distanceDatabase);

        LOGGER.info("分裂结果为：");
        LOGGER.info(ruleCluster1.toString());
        LOGGER.info(ruleCluster2.toString());

        // evaluate the bisecting
        double distanceSum1 = ruleCluster1.getDistanceSum();
        double distanceSum2 = ruleCluster2.getDistanceSum();
        double distanceSumNew = distanceSum1 + distanceSum2;
        double distanceSumOld = ruleCluster.getDistanceSum();
        LOGGER.info("分裂前的距离和：" + distanceSumOld + "  "
                + " 分裂后的距离和： " + distanceSumNew);
        if (distanceSumOld < distanceSumNew && distanceSumOld != 0.0D) {
            retRuleClusterList.add(ruleCluster);
        } else {
            retRuleClusterList.add(ruleCluster1);
            retRuleClusterList.add(ruleCluster2);
        }
        return retRuleClusterList;
    }

    /**
     * get the hash map of the distances between each rule-pair
     *
     * @param grammarRules the grammar rules
     * @return the distance database
     */
    public static HashMap<Integer, Map<Integer, Double>> getDistanceDatabase(GrammarRules grammarRules) {
        HashMap<Integer, Map<Integer, Double>> distanceDatabase = new HashMap<>();

        int[] ruleNumberList = new int[grammarRules.size()];
        int counter = 0;
        for (GrammarRuleRecord rule : grammarRules) {
            ruleNumberList[counter++] = rule.getRuleNumber();
        }
        for (int i = 1; i < grammarRules.size() - 1; i++) {
            GrammarRuleRecord ruleRecord1 = grammarRules.getRuleRecord(ruleNumberList[i]);
            int ruleIndex1 = ruleRecord1.ruleNumber();
            if (!distanceDatabase.containsKey(ruleIndex1)) {
                distanceDatabase.put(ruleIndex1, new TreeMap<>());
            }
            for (int j = i + 1; j < grammarRules.size(); j++) {
                GrammarRuleRecord ruleRecord2 = grammarRules.getRuleRecord(ruleNumberList[j]);
                int ruleIndex2 = ruleRecord2.ruleNumber();
                if (!distanceDatabase.containsKey(ruleIndex2)) {
                    distanceDatabase.put(ruleIndex2, new TreeMap<>());
                }
                String rule1 = ruleRecord1.getExpandedRuleString();
                String rule2 = ruleRecord2.getExpandedRuleString();

                RuleDTW ruleDTW = new RuleDTW(rule1, rule2);
                double distance = ruleDTW.getDistance();

                distanceDatabase.get(ruleIndex1).put(ruleIndex2, distance);
                distanceDatabase.get(ruleIndex2).put(ruleIndex1, distance);
                LOGGER.info(rule1 + "  " + rule2 + " 相距 ： " + distance);
            }
        }

        return distanceDatabase;
    }


    /**
     * get the two new Center rule ids, that is split one cluster to two cluster
     *
     * @param distanceDatabase the distance database
     * @param ruleCluster      the current rule cluster
     * @return the two new Center rule ids
     */
    public static ArrayList<Integer> getNewCenterRuleId(HashMap<Integer, Map<Integer, Double>> distanceDatabase,
                                                        RuleCluster ruleCluster) {
        ArrayList<Integer> retCenterRuleIdList = new ArrayList<>();
        ArrayList<Integer> ruleIdsList = new ArrayList<>(ruleCluster.getRuleIds());
        ArrayList<Integer> newRuleCandidate = getMinSumOfDistanceRuleIdList(distanceDatabase, ruleIdsList);

        int maxCount = 0;
        int selectedFirstRuleId = -1;
        int selectedSecondRuleId = -1;
        for (Integer centerId : newRuleCandidate) {
            ArrayList<Integer> anotherNewRuleCandidate = getFarthestRuleIdList(distanceDatabase, centerId, ruleIdsList);
            // select the best new center according to the num of the second center candidate,
            // the more the num of the second center candidate, the better
            if (anotherNewRuleCandidate.size() > maxCount) {
                maxCount = anotherNewRuleCandidate.size();
                selectedFirstRuleId = centerId;
                selectedSecondRuleId = anotherNewRuleCandidate.get(0);
            }
        }

        // get the first new center
        retCenterRuleIdList.add(selectedFirstRuleId);
        // get the second new center
        retCenterRuleIdList.add(selectedSecondRuleId);
        return retCenterRuleIdList;
    }

    /**
     * find the rule list whose sum of distances to other rules extracted from the whole rules is minimum
     *
     * @param distanceDatabase the distance database
     * @param ruleIdsList      the rule ids in the same cluster
     * @return
     */
    public static ArrayList<Integer> getMinSumOfDistanceRuleIdList(HashMap<Integer, Map<Integer, Double>> distanceDatabase,
                                                                   ArrayList<Integer> ruleIdsList) {
        ArrayList<Integer> retRuleIdList = new ArrayList<>();

        Map<Double, ArrayList<Integer>> distanceTotalMap = new TreeMap<>();

        for (Integer key : ruleIdsList) {
            double dis = 0;
            for (Integer subKey : ruleIdsList) {
                if(key != subKey){
                    dis += distanceDatabase.get(key).get(subKey);
                }
            }
            if (!distanceTotalMap.containsKey(dis)) {
                ArrayList<Integer> keyArray = new ArrayList<>();
                keyArray.add(key);
                distanceTotalMap.put(dis, keyArray);
            } else {
                distanceTotalMap.get(dis).add(key);
            }
        }

//        for (Map.Entry<Double, ArrayList<Integer>> entry : distanceTotalMap.entrySet()) {
//            LOGGER.info(entry.getKey() + "  " + entry.getValue().toString());
//        }

        List<Map.Entry<Double, ArrayList<Integer>>> list = new ArrayList<>(distanceTotalMap.entrySet());

        if (null != list && list.size() > 0) {
            retRuleIdList.addAll(list.get(0).getValue());
        }
        return retRuleIdList;
    }

    /**
     * get the farthest rule id list
     *
     * @param distanceDatabase the distance database
     * @param centerRuleId     the center rule id of the cluster
     * @param ruleIdsList      the rule ids in the same cluster
     * @return the farthest rule id list away from the Center Rule Id
     */
    public static ArrayList<Integer> getFarthestRuleIdList(HashMap<Integer, Map<Integer, Double>> distanceDatabase,
                                                           int centerRuleId, ArrayList<Integer> ruleIdsList) {
        ArrayList<Integer> retRuleIdList = new ArrayList<>();

        // sort the hashMap by the distance desc
        Map<Double, ArrayList<Integer>> distanceMap = new TreeMap<>();

        for (Integer key : ruleIdsList) {
            if (key == centerRuleId) {
                continue;
            }
//            LOGGER.info(centerRuleId + "  " + key
//                    + " " + distanceDatabase.get(centerRuleId).get(key));
            double dis = distanceDatabase.get(centerRuleId).get(key);
            if (!distanceMap.containsKey(dis)) {
                ArrayList<Integer> keyArray = new ArrayList<>();
                keyArray.add(key);
                distanceMap.put(dis, keyArray);
            } else {
                distanceMap.get(dis).add(key);
            }
        }
//        LOGGER.info("over");
//        for (Map.Entry<Double, ArrayList<Integer>> entry : distanceMap.entrySet()) {
//            LOGGER.info(entry.getKey() + "  " + entry.getValue().toString());
//        }

        List<Map.Entry<Double, ArrayList<Integer>>> list = new ArrayList<>(distanceMap.entrySet());

        if (null != list && list.size() > 0) {
            retRuleIdList = list.get(0).getValue();
        }
        return retRuleIdList;
    }

    public static boolean toSplit(RuleClusters rcPre, RuleClusters rcNex) {
        boolean ret = false;

        double aggPre = rcPre.getAggregation();
        double disPre = rcPre.getDispersion();

        double aggNex = rcNex.getAggregation();
        double disNex = rcNex.getDispersion();

        if((aggNex > aggPre && disNex < disPre)){
            LOGGER.info("aggPre : " + aggPre);
            LOGGER.info("disPre : " + disPre);
            LOGGER.info("aggNex : " + aggNex);
            LOGGER.info("disNex : " + disNex);
            if(aggNex > aggPre){
                LOGGER.info("aggNex > aggPre");
            }
            if(disNex < disPre){
                LOGGER.info("disNex < disPre");
            }
            return false;
        }

        double currentElevator = Math.abs(aggPre - aggNex) / Math.abs(disNex - disPre);

        if (elevator == 0.0D) {
            elevator = currentElevator;
            return true;
        }

        double e = log2(currentElevator) - log2(elevator);
        LOGGER.info("e = " + e);
        if (e < 1) {
            ret = true;
            elevator = currentElevator;
        }
        return ret;
    }

    static public double log2(double value) {
        return Math.log(value) / Math.log(2);
    }
}
