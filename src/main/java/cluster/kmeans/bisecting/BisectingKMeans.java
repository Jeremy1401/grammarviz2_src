package cluster.kmeans.bisecting;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.*;

import net.seninp.gi.logic.GrammarRuleRecord;
import net.seninp.gi.logic.GrammarRules;

/**
 * Bisecting k-means clustering algorithm.
 *
 * @author Jeremy
 */
public class BisectingKMeans {
    private static final Log LOGGER = LogFactory.getLog(BisectingKMeans.class);
    private final int k;  // the number of clustering
    private final int m;  // times of bisecting trials
    private final float maxMovingPointRate;
    private final Set<Integer> centroidSet = Sets.newTreeSet();
    private GrammarRules grammarRules;

    void setGrammarRules(GrammarRules grammarRules){
        this.grammarRules = grammarRules;
    }

    GrammarRules getGrammarRules(){
        return this.grammarRules;
    }

    public BisectingKMeans(int k, int m, float maxMovingPointRate) {
        this.k = k;
        this.m = m;
        this.maxMovingPointRate = maxMovingPointRate;
    }

//    public void clustering() throws Exception{
//        final int bisectingK = 2;
//        int bisectingIterations = 0;
//        int maxInterations = 20;
//        if(grammarRules.size() == 0 || grammarRules == null){
//            LOGGER.info("There is no grammar rule at all!");
//            return;
//        }
//        final Map<GrammarRuleRecord, Set<ClusterPoint<GrammarRuleRecord>>> clusteringPoints = Maps.newConcurrentMap();
//        while (clusteringPoints.size() <= k) {
//            LOGGER.info("Start bisecting iterations: #" + (++bisectingIterations) + ", bisectingK=" + bisectingK + ", maxMovingPointRate=" + maxMovingPointRate +
//                    ", maxInterations=" + maxInterations + ", parallism=" + parallism);
//
//            // for k=bisectingK, execute k-means clustering
//
//            // bisecting trials
//            KMeansClustering bestBisectingKmeans = null;
//            double minTotalSSE = Double.MAX_VALUE;
//            for (int i = 0; i < m; i++) {
//                final KMeansClustering kmeans = new KMeansClustering(bisectingK, maxMovingPointRate, maxInterations, parallism);
//                kmeans.initialize(points);
//                // the clustering result should have 2 clusters
//                kmeans.clustering();
//                double currentTotalSSE = computeTotalSSE(kmeans.getGrammarRuleRecordSet(), kmeans.getClusteringResult());
//                if (bestBisectingKmeans == null) {
//                    bestBisectingKmeans = kmeans;
//                    minTotalSSE = currentTotalSSE;
//                } else {
//                    if (currentTotalSSE < minTotalSSE) {
//                        bestBisectingKmeans = kmeans;
//                        minTotalSSE = currentTotalSSE;
//                    }
//                }
//                LOGGER.info("Bisecting trial <<" + i + ">> : minTotalSSE=" + minTotalSSE + ", currentTotalSSE=" + currentTotalSSE);
//            }
//            LOGGER.info("Best biscting: minTotalSSE=" + minTotalSSE);
//
//            // merge cluster points for choosing cluster bisected again
//            int id = generateNewClusterId(clusteringPoints.keySet());
//            Set<GrammarRuleRecord> bisectedCentroids = bestBisectingKmeans.getGrammarRuleRecordSet();
//            merge(clusteringPoints, id, bisectedCentroids, bestBisectingKmeans.getClusteringResult().getClusteredPoints());
//
//            if (clusteringPoints.size() == k) {
//                break;
//            }
//
//            // compute cluster to be bisected
//            ClusterInfo cluster = chooseClusterToBisect(clusteringPoints);
//            // remove centroid from collected clusters map
//            clusteringPoints.remove(cluster.centroidToBisect);
//            LOGGER.info("Cluster to be bisected: " + cluster);
//
//            points = Lists.newArrayList();
//            for (ClusterPoint<GrammarRuleRecord> cp : cluster.clusterPointsToBisect) {
//                points.add(cp.getPoint());
//            }
//
//            LOGGER.info("Finish bisecting iterations: #" + bisectingIterations + ", clusterSize=" + clusteringPoints.size());
//        }
//
//        // finally transform to result format
//        Iterator<Entry<GrammarRuleRecord, Set<ClusterPoint<GrammarRuleRecord>>>> iter = clusteringPoints.entrySet().iterator();
//        while (iter.hasNext()) {
//            Entry<GrammarRuleRecord, Set<ClusterPoint<GrammarRuleRecord>>> entry = iter.next();
//            clusteredPoints.put(entry.getKey().getId(), entry.getValue());
//            centroidSet.add(entry.getKey());
//        }
//    }
//
//    private void merge(final Map<GrammarRuleRecord, Set<ClusterPoint<GrammarRuleRecord>>> clusteringPoints,
//                       int id, Set<GrammarRuleRecord> bisectedCentroids,
//                       Map<Integer, Set<ClusterPoint<GrammarRuleRecord>>> bisectedClusterPoints) {
//        int startId = id;
//        for (GrammarRuleRecord centroid : bisectedCentroids) {
//            Set<ClusterPoint<GrammarRuleRecord>> set = bisectedClusterPoints.get(centroid.getId());
//            centroid.setId(startId);
//            // here, we don't update cluster id for ClusterPoint object in set,
//            // we should do it until iterate the set for choosing cluster to be bisected
//            clusteringPoints.put(centroid, set);
//            startId++;
//        }
//    }
//
//    private int generateNewClusterId(Set<GrammarRuleRecord> keptCentroids) {
//        int id = -1;
//        for (GrammarRuleRecord centroid : keptCentroids) {
//            if (centroid.getId() > id) {
//                id = centroid.getId();
//            }
//        }
//        return id + 1;
//    }
//
//    private ClusterInfo chooseClusterToBisect(Map<GrammarRuleRecord, Set<ClusterPoint<GrammarRuleRecord>>> clusteringPoints) {
//        double maxSSE = 0.0;
//        int clusterIdWithMaxSSE = -1;
//        GrammarRuleRecord centroidToBisect = null;
//        Set<ClusterPoint<GrammarRuleRecord>> clusterToBisect = null;
//        Iterator<Entry<GrammarRuleRecord, Set<ClusterPoint<GrammarRuleRecord>>>> iter = clusteringPoints.entrySet().iterator();
//        while (iter.hasNext()) {
//            Entry<GrammarRuleRecord, Set<ClusterPoint<GrammarRuleRecord>>> entry = iter.next();
//            GrammarRuleRecord centroid = entry.getKey();
//            Set<ClusterPoint<GrammarRuleRecord>> cpSet = entry.getValue();
//            double sse = computeSSE(centroid, cpSet);
//            if (sse > maxSSE) {
//                maxSSE = sse;
//                clusterIdWithMaxSSE = centroid.getId();
//                centroidToBisect = centroid;
//                clusterToBisect = cpSet;
//            }
//        }
//        return new ClusterInfo(clusterIdWithMaxSSE, centroidToBisect, clusterToBisect, maxSSE);
//    }
//
//    private double computeTotalSSE(Set<GrammarRuleRecord> centroids, ClusteringResult<GrammarRuleRecord> clusteringResult) {
//        double sse = 0.0;
//        for (GrammarRuleRecord center : centroids) {
//            int clusterId = center.getId();
//            for (ClusterPoint<GrammarRuleRecord> p : clusteringResult.getClusteredPoints().get(clusterId)) {
//                double distance = MetricUtils.euclideanDistance(p.getPoint(), center);
//                sse += distance * distance;
//            }
//        }
//        return sse;
//    }
//
//    private double computeSSE(GrammarRuleRecord centroid, Set<ClusterPoint<GrammarRuleRecord>> cpSet) {
//        double sse = 0.0;
//        for (ClusterPoint<GrammarRuleRecord> cp : cpSet) {
//            // update cluster id for ClusterPoint object
//            cp.setClusterId(centroid.getId());
//            double distance = MetricUtils.euclideanDistance(cp.getPoint(), centroid);
//            sse += distance * distance;
//        }
//        return sse;
//    }
//
//    private class ClusterInfo {
//
//        private final int id;
//        private final GrammarRuleRecord centroidToBisect;
//        private final Set<ClusterPoint<GrammarRuleRecord>> clusterPointsToBisect;
//        private final double maxSSE;
//
//        public ClusterInfo(int id, GrammarRuleRecord centroidToBisect, Set<ClusterPoint<GrammarRuleRecord>> clusterPointsToBisect, double maxSSE) {
//            super();
//            this.id = id;
//            this.centroidToBisect = centroidToBisect;
//            this.clusterPointsToBisect = clusterPointsToBisect;
//            this.maxSSE = maxSSE;
//        }
//
//        @Override
//        public String toString() {
//            return "ClusterInfo[id=" + id + ", points=" + clusterPointsToBisect.size() + ", maxSSE=" + maxSSE + "]";
//        }
//    }
//
//    public Set<GrammarRuleRecord> getCentroidSet() {
//        return centroidSet;
//    }
//
//    public static void main(String[] args) {
//        int k = 10;
//        int m = 25;
//        float maxMovingPointRate = 0.01f;
//        int parallism = 5;
//        BisectingKMeans bisecting = new BisectingKMeans(k, m, maxMovingPointRate, parallism);
////		File dir = FileUtils.getDataRootDir();
//        bisecting.setInputFiles(new File("/Users/Jeremy/Desktop/毕设/参考文献/导师推荐/代码/dm-clustering-master/dm-clustering-kmeans/src/main/data/points.txt"));
//        bisecting.clustering();
//
//        System.out.println("== Clustered points ==");
//        ClusteringResult<GrammarRuleRecord> result = bisecting.getClusteringResult();
//        ClusteringUtils.print2DClusterPoints(result.getClusteredPoints());
//
//        // print centroids
//        System.out.println("== Centroid points ==");
//        for (GrammarRuleRecord p : bisecting.getCentroidSet()) {
//            System.out.println(p.getX() + "," + p.getY() + "," + p.getId());
//        }
//    }

}
