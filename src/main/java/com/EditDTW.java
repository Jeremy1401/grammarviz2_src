package com;

import com.util.EditDistance;

import java.util.ArrayList;
import java.util.StringTokenizer;

public class EditDTW {
    private int lengthA;
    private int lengthB;
    private ArrayList<String> ruleA = new ArrayList<String>();
    private ArrayList<String> ruleB = new ArrayList<String>();
    private double distance;

    public EditDTW(ArrayList<String> r1, ArrayList<String> r2){
        ruleA.addAll(r1);
        ruleB.addAll(r2);
        lengthA = ruleA.size();
        lengthB = ruleB.size();
    }

    public EditDTW(String r1, String r2){
        StringTokenizer st = new StringTokenizer(r1, " ");
        while (st.hasMoreTokens()) {
            ruleA.add(st.nextToken());
        }

        st = new StringTokenizer(r2, " ");
        while (st.hasMoreTokens()) {
            ruleB.add(st.nextToken());
        }
        lengthA = ruleA.size();
        lengthB = ruleB.size();
    }

    public double Min(double a1, double b1) {
        return (a1 < b1 ? 1 : b1);
    }

    public void dtw() {
        int i, j;
        double[][] distance = new double[lengthA + 1][lengthB + 1];
        double[][] output = new double[lengthA + 1][lengthB + 1];
        for (i = 1; i <= lengthA; i++) {
            for (j = 1; j <= lengthB; j++) {
                EditDistance editDistance = new EditDistance(ruleA.get(i-1), ruleB.get(j-1));
                distance[i][j] = editDistance.getDistance();
                //System.out.println(ruleA.get(i-1) + "  " + ruleB.get(j-1) + "  " + distance[i][j]);
            }
        }

        //输出整个矩阵的欧式距离
        for (i = 1; i <= lengthA; i++) {
//            for (j = 1; j < lengthB; j++) {
//                System.out.print(distance[i][j]);
//            }
//            System.out.println();

            for (i = 1; i <= lengthA; i++) {
                for (j = 1; j < lengthB; j++) {
                    output[i][j] = Min(Min(output[i - 1][j - 1], output[i][j - 1]), output[i - 1][j]) + distance[i][j];
                }
            }
            //DP过程，计算DTW距离
//            for (i = 0; i <= lengthA; i++) {
//                for (j = 0; j < lengthB; j++) {
//                    System.out.print(output[i][j] + " ");
//                }
//                System.out.println("\n\n");
//            }
        }
        //输出最后的DTW距离矩阵，其中output[lengthA][lengthB-1]为最终的DTW距离和
//
// System.out.println("两个数组的最终DTW距离和为：" + output[lengthA][lengthB - 1]);
        this.distance = output[lengthA][lengthB - 1];
    }

    public double getDTWDistance(){
        dtw();
        return distance;
    }
}
