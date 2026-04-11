package de.hpi.isg.schedules;

public final class ScheduledDenialConstraint extends IntermediateMaintainer {
    public ScheduledDenialConstraint(int numPredicates, int dcIdx) {
        super(numPredicates);
        this.dcIdx = dcIdx;
    }
    public boolean isReflexive = false;

    public final int dcIdx;
}
