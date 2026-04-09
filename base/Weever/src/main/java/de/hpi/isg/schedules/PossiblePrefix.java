package de.hpi.isg.schedules;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class PossiblePrefix implements Comparable<PossiblePrefix> {
    public HashSet<IntermediateMaintainer>[] stillPartOf;
    public HashSet<PossiblePrefix>[] subsetOf;
    public ScheduledDenialConstraint owner;
    public int lastPredIdx = 0;
    public ScheduledPrefix scheduledPrefix;

    public PossiblePrefix(ScheduledDenialConstraint owner) {
        this.stillPartOf = new HashSet[owner.predicateIndices.length];
        this.subsetOf = new HashSet[owner.predicateIndices.length];
        this.owner = owner;
    }

    @Override
    public int compareTo(@NotNull PossiblePrefix possiblePrefix) {
        if (this == possiblePrefix || this.equals(possiblePrefix)) {
            return 0;
        } else {
            int intA = this.getNextActivePredicate(0);
            int intB = possiblePrefix.getNextActivePredicate(0);

            while (intA != -1 && intB != -1) {
                if (intA != intB) {
                    return intA - intB;
                }

                intA = this.getNextActivePredicate(intA + 1);
                intB = possiblePrefix.getNextActivePredicate(intB + 1);
            }

            if (intB == -1) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PossiblePrefix that = (PossiblePrefix) o;

        return Arrays.equals(stillPartOf, that.stillPartOf);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(stillPartOf);
    }

    public int getNextActivePredicate(int start) {
        for (int i = start; i < stillPartOf.length; i++) {
            if (stillPartOf[i] != null) return i;
        }
        return -1;
    }

    @Override
    public String toString() {
        ArrayList<Integer> indices = new ArrayList<>();
        for (int predIdx = getNextActivePredicate(0); predIdx != -1; predIdx = getNextActivePredicate(predIdx + 1)) {
            indices.add(predIdx);
        }
        return indices.toString();
    }
}
