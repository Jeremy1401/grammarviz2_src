package net.seninp.gi.logic;

import java.io.Serializable;

/**
 * Helper class implementing an interval used when plotting.
 *
 * @author Manfred Lerner, seninp
 */
public class RuleInterval implements Comparable<RuleInterval>, Cloneable, Serializable {

    public int id; // the corresponding rule id
    public int startPos; // interval start
    public int endPos; // interval stop
    public double coverage; // coverage or any other sorting criterion

    public RuleInterval() {
        super();
        this.id = -1;
        this.startPos = -1;
        this.endPos = -1;
    }

    public RuleInterval(int startPos, int endPos) {
        super();
        this.id = -1;
        this.startPos = startPos;
        this.endPos = endPos;
        this.coverage = Double.NaN;
    }

    public RuleInterval(int id, int startPos, int endPos) {
        super();
        this.id = id;
        this.startPos = startPos;
        this.endPos = endPos;
        this.coverage = Double.NaN;
    }

    public RuleInterval(int id, int startPos, int endPos, double coverage) {
        this.id = id;
        this.startPos = startPos;
        this.endPos = endPos;
        this.coverage = coverage;
    }

    public void setId(int ruleIndex) {
        this.id = ruleIndex;
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @param startPos starting position within the original time series
     */
    public void setStart(int startPos) {
        this.startPos = startPos;
    }

    /**
     * @return starting position within the original time series
     */
    public int getStart() {
        return startPos;
    }

    /**
     * @param endPos ending position within the original time series
     */
    public void setEnd(int endPos) {
        this.endPos = endPos;
    }

    /**
     * @return ending position within the original time series
     */
    public int getEnd() {
        return endPos;
    }

    /**
     * @param coverage the coverage to set
     */
    public void setCoverage(double coverage) {
        this.coverage = coverage;
    }

    /**
     * @return the coverage
     */
    public double getCoverage() {
        return this.coverage;
    }

    public int getLength() {
        return this.endPos - this.startPos;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "[" + startPos + "-" + endPos + "]";
    }

    public int compareTo(RuleInterval arg0) {
        return Integer.compare(this.getStart(), Integer.valueOf(arg0.getStart()));
    }

    public RuleIntervalRelation relation(RuleInterval arg0) {
        RuleIntervalRelation ret = RuleIntervalRelation.NONE;
        int start0 = arg0.getStart();
        int end0 = arg0.getEnd();
//        System.out.println("start0" + " = " + start0);
//        System.out.println("end0" + " = " + end0);
//        System.out.println("start1" + " = " + startPos);
//        System.out.println("end1" + " = " + endPos);
        // equal
        if (startPos == start0 && endPos == end0) {
            return RuleIntervalRelation.EQUAL;
        }
        if ((startPos <= start0 && endPos >= end0)
                || (start0 <= startPos && end0 >= endPos)) {
            return RuleIntervalRelation.CONTAIN;
        }
        if (endPos < start0 || end0 < startPos) {
            return RuleIntervalRelation.SEPARATE;
        }
        if (endPos == start0 || end0 == startPos) {
            return RuleIntervalRelation.ADJACENT;
        }
        if (endPos > start0 || end0 > startPos) {
            return RuleIntervalRelation.INTERSECT;
        }

        return ret;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(coverage);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + endPos;
        result = prime * result + id;
        result = prime * result + startPos;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RuleInterval other = (RuleInterval) obj;
        if (Double.doubleToLongBits(coverage) != Double.doubleToLongBits(other.coverage))
            return false;
        if (endPos != other.endPos)
            return false;
        if (id != other.id)
            return false;
        if (startPos != other.startPos)
            return false;
        return true;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        RuleInterval clone = (RuleInterval) super.clone();
        clone.id = this.id;
        clone.startPos = this.startPos;
        clone.endPos = this.endPos;
        clone.coverage = this.coverage;
        return clone;
    }
}