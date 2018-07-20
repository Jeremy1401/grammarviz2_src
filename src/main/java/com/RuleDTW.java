package com;

/**
 * Created by dsm on 2018/7/9.
 */
public class RuleDTW {
    private int lengthA;
    private int lengthB;
    private String ruleA;
    private String ruleB;
    private double distance;

    public RuleDTW(String r1, String r2) {
        ruleA = r1;
        ruleB = r2;
        ruleA.replace(" ", "");
        ruleB.replace(" ", "");
        lengthA = ruleA.length();
        lengthB = ruleB.length();
    }

    public double Min(double a1, double b1) {
        return (a1 < b1 ? 1 : b1);
    }

    public double getDistance() {
        calDtw();
        return distance;
    }

    public void calDtw(){
        //     COST MATRIX:
        //   5|_|_|_|_|_|_|E| E = min Global Cost
        //   4|_|_|_|_|_|_|_| S = Start point
        //   3|_|_|_|_|_|_|_| each cell = min global cost to get to that point
        // j 2|_|_|_|_|_|_|_|
        //   1|_|_|_|_|_|_|_|
        //   0|S|_|_|_|_|_|_|
        //     0 1 2 3 4 5 6
        //            i
        //   access is M(i,j)... column-row

        final double[][] costMatrix = new double[ruleA.length()][ruleB.length()];
        final int maxI = ruleA.length()-1;
        final int maxJ = ruleB.length()-1;

        // Calculate the values for the first column, from the bottom up.
        costMatrix[0][0] = isModify(0, 0);
        for (int j=1; j<=maxJ; j++){
            costMatrix[0][j] = costMatrix[0][j-1] + isModify(0, j);
        }

        for (int i=1; i<=maxI; i++)   // i = columns
        {
            // Calculate the value for the bottom row of the current column
            //    (i,0) = LocalCost(i,0) + GlobalCost(i-1,0)
            costMatrix[i][0] = costMatrix[i-1][0] + isModify(i, 0);

            for (int j=1; j<=maxJ; j++)  // j = rows
            {
                // (i,j) = LocalCost(i,j) + minGlobalCost{(i-1,j),(i-1,j-1),(i,j-1)}
                final double minGlobalCost = Math.min(costMatrix[i-1][j], Math.min(costMatrix[i-1][j-1], costMatrix[i][j-1]));
                costMatrix[i][j] = minGlobalCost + isModify(i, j);
            }  // end for loop


        }  // end for loop

        //DP过程，计算DTW距离
        for (int i = 0; i < lengthA; i++) {
            for (int j = 0; j < lengthB; j++) {
                System.out.print(costMatrix[i][j] + " ");
            }
            System.out.println("\n\n");
        }
        // Minimum Cost is at (maxIi,maxJ)
        distance = costMatrix[maxI][maxJ];
    }

    private int isModify(int i, int j) {
        if (ruleA.charAt(i) == ruleB.charAt(j)) {
            return 0;
        } else {
            return 1;
            //return (Math.abs(ruleA.charAt(i) - ruleB.charAt(j)));
        }
    }

    /**
     * 测试
     *
     * @param args
     */
    public static void main(String[] args) {
        RuleDTW ruleDTW2 = new RuleDTW("bddcabcbcdaaccbdcaacbddcabcbddaaccbdcaac", "bcdaabbcddcaabbdddaabcdaabbcddcaabcdddaa");
        ruleDTW2.calDtw();
        System.out.println("rule dtw: " + ruleDTW2.getDistance());
    }
}
