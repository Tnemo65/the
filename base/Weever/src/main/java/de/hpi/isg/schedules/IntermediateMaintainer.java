package de.hpi.isg.schedules;

import de.hpi.isg.tidSets.TIdSet;

import de.hpi.isg.Operator;
import de.hpi.isg.tidSets.Operand;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

public abstract class IntermediateMaintainer implements Comparable<IntermediateMaintainer> {
    public final boolean[] predicateIndices;
    public TIdSet intermediate = null;
    boolean isFinished = false;

    // TODO: check helpfulness if time permits, would allow for more unifying (e.g., EQUAL and UNEQUAL)
    public TIdSet toRemove = null;

    protected IntermediateMaintainer(int numPredicates) {
        predicateIndices = new boolean[numPredicates];
    }

    private boolean isRemovingOperator(Operator op) {
        return op == Operator.UNEQUAL || op == Operator.GREATER || op == Operator.GREATER_EQUAL;
    }


    public void applyOn(Operand operand, Operator op, TIdSet allIds, int predIdx) {
        if (isFinished()) return;

        if (operand.equalTIds == null && operand.smallerTIds == null) {
            if (isRemovingOperator(op)) {
                if (intermediate == null) {
                    if (allIds.isEmpty()) {
                        markAsFinished(predIdx);
                        return;
                    } else {
                        intermediate = allIds.clone();
                    }
                }
            } else {
                markAsFinished(predIdx);
                return;
            }
        } else {
            if (intermediate == null) {
                if (isRemovingOperator(op)) {
                    intermediate = allIds.clone();
                    removeNotFulfillingTIds(intermediate, op, operand);
                } else {
                    intermediate = createIntermediate(op, operand);
                }
            } else {
                if (isRemovingOperator(op)) {
                    removeNotFulfillingTIds(intermediate, op, operand);
                } else {
                    retainFulfillingTIds(intermediate, op, operand);
                }
            }
        }

        if (intermediate == null || intermediate.isEmpty()) {
            markAsFinished(predIdx);
        }
    }

    private void removeNotFulfillingTIds(TIdSet intermediate, Operator op, Operand operand) {
        switch (op) {
            case UNEQUAL:
                intermediate.minus(operand.equalTIds);
                break;
            case GREATER:
                intermediate.minus(operand.smallerTIds);
                if (!operand.smallerAndEqual && operand.equalTIds != null)
                    intermediate.minus(operand.equalTIds);
                break;
            case GREATER_EQUAL:
                intermediate.minus(operand.smallerTIds);
                break;
        }
    }

    private TIdSet createIntermediate(Operator op, Operand operand) {
        switch (op) {
            case EQUAL:
                return operand.equalTIds.clone();
            case LESS:
                return operand.smallerTIds.clone();
            case LESS_EQUAL:
                var intermediate = operand.smallerTIds.clone();
                if (!operand.smallerAndEqual && operand.equalTIds != null)
                    intermediate.union(operand.equalTIds);
                return intermediate;
        }
        return null;
    }

    private void retainFulfillingTIds(TIdSet intermediate, Operator op, Operand operand) {
        switch (op) {
            case EQUAL:
                intermediate.intersect(operand.equalTIds);
                break;
            case LESS:
                intermediate.intersect(operand.smallerTIds);
                break;
            case LESS_EQUAL:
                TIdSet state;
                if (!operand.smallerAndEqual && operand.equalTIds != null) {
                    state = operand.smallerTIds.clone();
                    state.union(operand.equalTIds);
                } else {
                    state = operand.smallerTIds;
                }
                intermediate.intersect(state);
        }
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void markAsFinished(int predIdx) {
        intermediate = null;
        isFinished = true;
    }

    public void unfinish() {
        isFinished = false;
    }

    @Override
    public String toString() {
        ArrayList<Integer> indices = new ArrayList<>();
        for (int predIdx = getNextActivePredicate(0); predIdx != -1; predIdx = getNextActivePredicate(predIdx + 1)) {
            indices.add(predIdx);
        }
        return indices.toString();
    }

    public int getNextActivePredicate(int start) {
        for (int i = start; i < predicateIndices.length; i++) {
            if (predicateIndices[i]) return i;
        }
        return -1;
    }

    @Override
    public int compareTo(@NotNull IntermediateMaintainer other) {
        if (this == other) {
            return 0;
        } else {
            int intA = this.getNextActivePredicate(0);
            int intB = other.getNextActivePredicate(0);

            while (intA != -1 && intB != -1) {
                if (intA != intB) {
                    return intB - intA;
                }

                intA = this.getNextActivePredicate(intA + 1);
                intB = other.getNextActivePredicate(intB + 1);
            }

            if (intB == -1) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}
