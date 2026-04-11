package de.hpi.isg;

import de.hpi.isg.schedules.ScheduledDenialConstraint;
import de.hpi.isg.schedules.ScheduledPredicate;
import de.hpi.isg.schedules.ScheduledPrefix;

import java.util.ArrayList;

public class Schedule {
    ArrayList<ScheduledDenialConstraint> scheduledDcs;
    ArrayList<ScheduledPredicate> predicates;

    ArrayList<ScheduledPrefix> prefixes;
    int[] prefixIndices;
    int[] basePrefixIndices;

    int timeToReschedule = 2;
    int[] idxMapping;

    public Schedule(ArrayList<ScheduledDenialConstraint> scheduledDcs, ArrayList<ScheduledPredicate> predicates, ArrayList<ScheduledPrefix> prefixes, int[] prefixIndices) {
        this.scheduledDcs = scheduledDcs;
        this.predicates = predicates;
        this.prefixes = prefixes;
        this.prefixIndices = prefixIndices;
        this.basePrefixIndices = prefixIndices.clone();
    }

    public Schedule(ArrayList<ScheduledDenialConstraint> scheduledDcs, ArrayList<ScheduledPredicate> predicates) {
        this.scheduledDcs = scheduledDcs;
        this.predicates = predicates;
        this.prefixes = new ArrayList<>(0);
    }
}
