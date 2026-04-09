package de.hpi.isg.schedules;

import de.hpi.isg.Predicate;

import java.util.ArrayList;
import java.util.HashSet;

public class ScheduledPredicate extends Predicate {
    public HashSet<IntermediateMaintainer> belongsTo;

    public ArrayList<ScheduledDenialConstraint> baseDcs;

    boolean hasBeenExecuted = false;

    public ScheduledPredicate tPrimePredicate;

    public ScheduledPredicate(Predicate p, ArrayList<ScheduledDenialConstraint> baseDcs, ScheduledPredicate tPrimePredicate) {
        super(p.queryColumnIdx, p.probeColumnIdx, p.op.tPrimeVersion, p.index);
        init(baseDcs, tPrimePredicate);
    }

    public ScheduledPredicate(Predicate p, ArrayList<ScheduledDenialConstraint> baseDcs) {
        super(p.probeColumnIdx, p.queryColumnIdx, p.op, p.index);
        init(baseDcs, null);
    }

    private void init(ArrayList<ScheduledDenialConstraint> baseDcs, ScheduledPredicate tPrimePredicate) {
        belongsTo = new HashSet<>(baseDcs);
        this.baseDcs = baseDcs;
        this.tPrimePredicate = tPrimePredicate;
    }

    public boolean execute() {
        if (hasBeenExecuted) return true;

        hasBeenExecuted = true;
        if (tPrimePredicate != null) {
            tPrimePredicate.hasBeenExecuted = true;
        }
        return false;
    }

    public void unexecute() {
        hasBeenExecuted = false;
    }
}
