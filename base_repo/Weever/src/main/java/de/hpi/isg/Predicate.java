package de.hpi.isg;

public class Predicate {
    public final int probeColumnIdx;
    public final int queryColumnIdx;
    public final Operator op;
    public final int index;

    // t.probeColumnIdx op newT.queryColumnIdx

    public Predicate(int probeColumnIdx, int queryColumnIdx, Operator op) {
        this.probeColumnIdx = probeColumnIdx;
        this.queryColumnIdx = queryColumnIdx;
        this.op = op;
        this.index = 0;
    }

    public Predicate(int probeColumnIdx, int queryColumnIdx, Operator op, int index) {
        this.probeColumnIdx = probeColumnIdx;
        this.queryColumnIdx = queryColumnIdx;
        this.op = op;
        this.index = index;
    }

    public boolean isSingleColumnPredicate() {
        return probeColumnIdx == queryColumnIdx;
    }

    public boolean isSingleMapAccessible() {
        return op.equals(Operator.EQUAL) || op.equals(Operator.UNEQUAL);
    }

    public boolean isReflexive() {
        return isSingleColumnPredicate() && isSingleMapAccessible();
    }

    public int getClassIdx() {
        if (isSingleColumnPredicate()) {
            switch (op) {
                case EQUAL:
                    return 0;
                case UNEQUAL:
                    return 2;
                default:
                    return 4; // all inequalities
            }
        } else {
            switch (op) {
                case EQUAL:
                    return 1;
                case UNEQUAL:
                    return 3;
                default:
                    return 5; // all inequalities
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Predicate)) return false;

        Predicate predicate = (Predicate) o;

        if (probeColumnIdx != predicate.probeColumnIdx) return false;
        if (queryColumnIdx != predicate.queryColumnIdx) return false;
        return op == predicate.op;
    }

    @Override
    public int hashCode() {
        int result = probeColumnIdx;
        result = 31 * result + queryColumnIdx;
        result = 31 * result + op.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "t." + probeColumnIdx + " " + op + " t'." + queryColumnIdx;
    }
}
