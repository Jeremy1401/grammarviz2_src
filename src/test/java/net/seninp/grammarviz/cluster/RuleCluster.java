package net.seninp.grammarviz.cluster;

import net.seninp.gi.logic.GrammarRules;
import net.seninp.gi.logic.RuleInterval;
import net.seninp.gi.logic.RuleIntervalRelation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RuleCluster {
    private int centerRuleId;  // the rule id of the center in this cluster
    private ArrayList<Integer> ruleIds = new ArrayList<>();  // the rule ids in this cluster including the center rule Id
    private ArrayList<RuleInterval> ruleIntervals = new ArrayList<>();
    private double distanceSum;  // distanceSum
    private int minRuleLen = Integer.MAX_VALUE;
    private int maxRuleLen = -1;

    public RuleCluster() {
        centerRuleId = 0;
        ruleIds = new ArrayList<>();
        distanceSum = 0.0D;
    }

    public RuleCluster(int cid, ArrayList<Integer> ids, double criterion) {
        centerRuleId = cid;
        ruleIds.addAll(ids);
        distanceSum = criterion;
    }

    public RuleCluster(ArrayList<Integer> idsList) {
        centerRuleId = -1;
        ruleIds.addAll(idsList);
        distanceSum = -1;
    }

    public void setCenterId(int centerRuleId) {
        this.centerRuleId = centerRuleId;
        if (!ruleIds.contains(centerRuleId)) {
            ruleIds.add(centerRuleId);
        }
        distanceSum = 0.0D;
    }

    public int getCenterId() {
        return this.centerRuleId;
    }

    public void setRuleIds(ArrayList<Integer> ruleIds) {
        this.ruleIds = ruleIds;
    }

    public ArrayList<Integer> getRuleIds() {
        return this.ruleIds;
    }

    public void setRuleIntervals(ArrayList<RuleInterval> ruleIntervals) {
        for (RuleInterval ruleInterval : ruleIntervals) {
            this.ruleIntervals.add(ruleInterval);
            int length = ruleInterval.getLength();
            if (length > maxRuleLen) {
                maxRuleLen = length;
            } else if (length < minRuleLen) {
                minRuleLen = length;
            }
        }
    }

    public ArrayList<RuleInterval> getRuleIntervals() {
        return this.ruleIntervals;
    }

    public void addRuleInterval(RuleInterval ruleInterval) {
        this.ruleIntervals.add(ruleInterval);
        int length = ruleInterval.getLength();
        if (length > maxRuleLen) {
            maxRuleLen = length;
        }
        if (length < minRuleLen) {
            minRuleLen = length;
        }
    }

    public void addRuleIntervals(ArrayList<RuleInterval> intervals) {
        if (intervals == null || intervals.size() == 0) {
            return;
        }
        for (RuleInterval ruleInterval : intervals) {
            this.ruleIntervals.add(ruleInterval);
            int length = ruleInterval.getLength();
            if (length > maxRuleLen) {
                maxRuleLen = length;
            }
            if (length < minRuleLen) {
                minRuleLen = length;
            }
        }
    }

    public void reorganizeRuleIntervals() {
        ArrayList<RuleInterval> temp = new ArrayList<>();
        temp.addAll(ruleIntervals);
        Collections.sort(temp);
        for(RuleInterval ruleInterval : temp){
            System.out.println("ruleInterval: " + ruleInterval.toString());
        }
        ruleIntervals.clear();
        ruleIntervals.add(temp.get(0));
        for (int i = 1; i < temp.size(); i++) {
            System.out.println("ruleIntervals size: " + ruleIntervals.size());
            boolean insertFlag = false;
            for (RuleInterval ruleInterval1 : ruleIntervals) {
                RuleIntervalRelation relation = temp.get(i).relation(ruleInterval1);

                System.out.println(temp.get(i).toString());
                System.out.println(ruleInterval1.toString());
                System.out.println("关系为：" + relation.toString());

                if (relation == RuleIntervalRelation.NONE || relation == RuleIntervalRelation.EQUAL) {
                    insertFlag = false;
                    break;
                }
                if (relation == RuleIntervalRelation.INTERSECT
                        || relation == RuleIntervalRelation.ADJACENT
                        || relation == RuleIntervalRelation.CONTAIN) {
                    int start1 = ruleInterval1.getStart();
                    int end1 = ruleInterval1.getEnd();
                    int start2 = temp.get(i).getStart();
                    int end2 = temp.get(i).getEnd();
                    int start0 = Integer.min(start1, start2);
                    int end0 = Integer.max(end1, end2);
                    System.out.println("\nruleInterval: " + ruleInterval1.toString());
                    System.out.println("替换为");
                    ruleInterval1.setStart(start0);
                    ruleInterval1.setEnd(end0);
                    System.out.println("ruleInterval: " + ruleInterval1.toString() + "\n");
                    insertFlag = false;
                    break;
                }
                if (relation == RuleIntervalRelation.SEPARATE) {
                    insertFlag = true;
                }
            }

            if(insertFlag){
                System.out.println("插入" + temp.get(i));
                ruleIntervals.add(temp.get(i));
            }
            for(RuleInterval ruleInterval : ruleIntervals){
                System.out.println("ruleInterval: " + ruleInterval.toString());
            }
        }
        System.out.println("\n排序后：");
        Collections.sort(ruleIntervals);
        for(RuleInterval ruleInterval : ruleIntervals){
            System.out.println("ruleInterval: " + ruleInterval.toString());
        }
        System.out.println("\n");
        // to do

    }

    public void setDistanceSum(double distanceSum) {
        this.distanceSum = distanceSum;
    }

    public double getDistanceSum() {
        return this.distanceSum;
    }

    public void setMinRuleLen(int minRuleLen) {
        this.minRuleLen = minRuleLen;
    }

    public int getMinRuleLen() {
        return this.minRuleLen;
    }

    public void setMaxRuleLen(int maxRuleLen) {
        this.maxRuleLen = maxRuleLen;
    }

    public int getMaxRuleLen() {
        return this.maxRuleLen;
    }

    public void calculateDistanceSum(HashMap<Integer, Map<Integer, Double>> distanceDatabase) {
        for (int i = 0; i < ruleIds.size() - 1; i++) {
            for (int j = i + 1; j < ruleIds.size(); j++) {
                distanceSum += distanceDatabase.get(ruleIds.get(i)).get(ruleIds.get(j));
            }
        }
    }

    public void addRuleIds(ArrayList<Integer> ruleIds) {
        this.ruleIds.addAll(ruleIds);
    }

    public void addRuleId(int id) {
        ruleIds.add(id);
    }

    public void addRuleId(int id, double distance) {
        ruleIds.add(id);
        distanceSum += distance;
    }

    public void removeRuleId(int id) {
        ruleIds.remove(id);
        for (int i = 0; i < ruleIntervals.size(); i++) {
            if (ruleIntervals.get(i).getId() == id) {
                ruleIntervals.remove(i);
            }
        }
    }

    public void clear() {
        centerRuleId = -1;
        ruleIds.clear();
        distanceSum = -1;
        ruleIntervals.clear();
    }

    public int getSubsquenceNum(GrammarRules grammarRules) {
        int ret = 0;
        for (Integer id : ruleIds) {
            ret += grammarRules.get(id).getOccurrences().size();
        }
        return ret;
    }

    public String toString() {
        String ret = "center : " + this.centerRuleId + " -> ["
                + ruleIds.toString() + "], 共 " + ruleIds.size()
                + "个， 距离和为： " + distanceSum + " 间隔信息为：\n";
        for (RuleInterval interval : ruleIntervals) {
            ret += "R" + interval.getId() + " : "
                    + interval.toString() + "   "
                    + interval.getCoverage() + "\n";
        }
        return ret;
    }
}
