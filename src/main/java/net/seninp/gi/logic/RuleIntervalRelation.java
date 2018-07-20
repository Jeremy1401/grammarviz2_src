package net.seninp.gi.logic;

public enum RuleIntervalRelation {
    NONE(0),
    CONTAIN(1),
    EQUAL(2),
    INTERSECT(3),
    ADJACENT(4),
    SEPARATE(5);

    private final int relationIndex;

    RuleIntervalRelation(int algIdx) {
        this.relationIndex = algIdx;
    }

    public int toRelationIndex() {
        return this.relationIndex;
    }

    public String toString() {
        switch (this.relationIndex) {
            case 0:
                return "NONE";
            case 1:
                return "CONTAIN";
            case 2:
                return "EQUAL";
            case 3:
                return "INTERSECT";
            case 4:
                return "ADJACENT";
            case 5:
                return "SEPARATE";
            default:
                throw new RuntimeException("Unknown index:" + this.relationIndex);
        }
    }
}
