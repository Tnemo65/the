package de.hpi.isg.schedules;

import de.hpi.isg.Operator;
import de.hpi.isg.tidSets.Operand;
import de.hpi.isg.tidSets.TIdSet;

public final class ScheduledPrefix extends IntermediateMaintainer implements Cloneable {
    public IntermediateMaintainer[][] stillPartOf;
    final public IntermediateMaintainer[][] endOf;
    final int lastPredIdx;
    int[] prefixIndices;

    public ScheduledPrefix(PossiblePrefix possiblePrefix, int[] prefixIndices) {
        super(possiblePrefix.stillPartOf.length);
        this.stillPartOf = new IntermediateMaintainer[possiblePrefix.stillPartOf.length][];
        this.endOf = new IntermediateMaintainer[possiblePrefix.stillPartOf.length][];
        this.lastPredIdx = possiblePrefix.lastPredIdx;
        this.prefixIndices = prefixIndices;
    }

    @Override
    public void applyOn(Operand operand, Operator op, TIdSet allIds, int predIdx) {
        super.applyOn(operand, op, allIds, predIdx);
        if (!isFinished) {
            processEndsOfPrefix(predIdx, allIds);
        }
    }

    @Override
    public void markAsFinished(int predIdx) {
        super.markAsFinished(predIdx);

        for (int predIterIdx = getNextActivePredicate(predIdx + 1); predIterIdx != -1; predIterIdx = getNextActivePredicate(predIterIdx + 1)) {
            prefixIndices[predIterIdx] -= 1;
        }

        if (stillPartOf[predIdx] != null) {
            for (var intermediateMaintainer : stillPartOf[predIdx]) {
                intermediateMaintainer.markAsFinished(predIdx);
            }
        }
    }

    public void processEndsOfPrefix(int predIdx, TIdSet allIds) {
        IntermediateMaintainer directAssignee = null;
        if (lastPredIdx == predIdx) {
            for (var intermediateMaintainer : endOf[predIdx]) {
                if (intermediateMaintainer.intermediate == null && !intermediateMaintainer.isFinished) {
                    directAssignee = intermediateMaintainer;
                    intermediateMaintainer.intermediate = this.intermediate;
                    break;
                }
            }
        }
        for (var intermediateMaintainer : endOf[predIdx]) {
            if (intermediateMaintainer != directAssignee) {
                intermediateMaintainer.applyOn(new Operand(intermediate), Operator.EQUAL, allIds, predIdx);
            }
        }
        if (lastPredIdx == predIdx) {
            intermediate = null;
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }
}
