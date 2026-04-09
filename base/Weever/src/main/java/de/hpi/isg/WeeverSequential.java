package de.hpi.isg;

import de.hpi.isg.dataStructures.ltAggregateMap.LTAggregateMap;
import de.hpi.isg.schedules.ScheduledDenialConstraint;
import de.hpi.isg.schedules.ScheduledPredicate;
import de.hpi.isg.tidSets.Operand;
import de.hpi.isg.tidSets.TIdSet;
import it.unimi.dsi.fastutil.floats.Float2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public final class WeeverSequential extends Weever {
    Schedule[] schedule;
    PredicateScheduler predicateScheduler = new PredicateScheduler();
    final int[] columnCardinalities;

    ArrayList<ArrayList<Integer>> allPredCounts = new ArrayList<>(1000);
    ArrayList<ArrayList<Long>> allTimes = new ArrayList<>(1000);

    ArrayList<int[]> allColumnCardinalities = new ArrayList<>(1000);
    ArrayList<ArrayList<int[]>> allPermutations = new ArrayList<>(1000);

    public WeeverSequential(ArrayList<DenialConstraint> denialConstraints, int[] keyColumns, ArrayList<String> columnTypes) {
        super(denialConstraints, keyColumns, columnTypes);

        columnCardinalities = new int[this.usedColumns.length];
        Arrays.fill(columnCardinalities, 1);
        predicateScheduler.initDataStructures(denialConstraints);
        schedule = predicateScheduler.scheduleFirstTime(columnCardinalities);
//        schedule = predicateScheduler.initPermutations();
//
//        int i = 0;
//        for (var dc : denialConstraints) {
//            for (var p : dc.predicates) {
//                Utils.pred2Id.put(p, i++);
//            }
//        }
//        Utils.timePerPredicate = new long[i];
//        Utils.callsPerPredicate = new long[i];
    }

    Operand getOperandForSingleMap(Number tupleVal, int colIdx) {
        return new Operand(columnValues2Tid.get(colIdx).get(tupleVal));
    }

    Operand getOperandForMultiMapSingleCol(Number tupleVal, int colIdx) {
        LTAggregateMap ltAggregateMap = (LTAggregateMap) columnValues2Tid.get(colIdx);
        return ltAggregateMap.findSmallerTIds(tupleVal, false);
    }

    Operand getOperandForMultiMapMultiCol(Number tupleVal, int colIdx, Operator op) {
        LTAggregateMap ltAggregateMap = (LTAggregateMap) columnValues2Tid.get(colIdx);
        if (op == Operator.GREATER || op == Operator.LESS_EQUAL) {
            return ltAggregateMap.findSmallerTIds(tupleVal, true);
        } else {
            return ltAggregateMap.findSmallerTIds(tupleVal, false);
        }
    }


    private Operand getOperandsFor(ScheduledPredicate predicate, Number[] tuple) {
        if (predicate.isSingleColumnPredicate()) {
            Number tupleVal = tuple[predicate.queryColumnIdx];
            if (predicate.isSingleMapAccessible()) {
                return getOperandForSingleMap(tupleVal, predicate.probeColumnIdx);
            } else {
                if (predicate.tPrimePredicate == null) {
                    return getOperandForMultiMapMultiCol(tupleVal, predicate.probeColumnIdx, predicate.op);
                } else {
                    return getOperandForMultiMapSingleCol(tupleVal, predicate.probeColumnIdx);
                }
            }
        } else {
            Number tupleVal = tuple[predicate.queryColumnIdx];
            if (predicate.isSingleMapAccessible()) {
                return getOperandForSingleMap(tupleVal, predicate.probeColumnIdx);
            } else {
                return getOperandForMultiMapMultiCol(tupleVal, predicate.probeColumnIdx, predicate.op);
            }
        }
    }

    void executeAllForDenialConstraint(Schedule currSideSchedule, ScheduledDenialConstraint denialConstraint, Number[] tuple) {
        if (denialConstraint.isFinished()) return;

        for (int predIdx = denialConstraint.getNextActivePredicate(0); predIdx != -1; predIdx = denialConstraint.getNextActivePredicate(predIdx + 1)) {
            var predicate = currSideSchedule.predicates.get(predIdx);
            if (processPredicate(predicate, tuple, currSideSchedule, predIdx)) continue;
            if (denialConstraint.isFinished()) {
                break;
            }
        }
    }

    final void validateSingleSide(Schedule schedule, int tId, Number[] tuple, boolean isTPrime) {
        if (!schedule.prefixes.isEmpty()) {
            executeAllPrefixes(schedule, tuple);
        }
        for (var scheduledDc : schedule.scheduledDcs) {
            executeAllForDenialConstraint(schedule, scheduledDc, tuple);
            if (!scheduledDc.isFinished()) {
                if (Utils.storeViolations) {
                    // O (1)
                    if (isTPrime) {
                        finalTPrimeMatches.get(scheduledDc.dcIdx).put(tId, scheduledDc.intermediate);
                    } else {
                        finalTMatches.get(scheduledDc.dcIdx).put(tId, scheduledDc.intermediate);
                        if (scheduledDc.isReflexive) {
                            finalTPrimeMatches.get(scheduledDc.dcIdx).put(tId, scheduledDc.intermediate);
                        }
                    }
                }
                scheduledDc.intermediate = null;
            }
        }
    }

    private void executeAllPrefixes(Schedule schedule, Number[] tuple) {
        for (int predIdx = 0; predIdx < schedule.prefixIndices.length; predIdx++) {
            if (schedule.prefixIndices[predIdx] > 0) {
                var predicate = schedule.predicates.get(predIdx);
                processPredicate(predicate, tuple, schedule, predIdx);
            }
        }
    }

    void validateSingleTuple(int tId, Number[] tuple) {
        // first tVersion because that benefits tPrimeVersion
        validateSingleSide(schedule[0], tId, tuple, false);
        validateSingleSide(schedule[1], tId, tuple, true);
        insertValuesIntoIndexes(tId, tuple);
        reschedule();
        allIds.add(tId);
    }

    private void insertValuesIntoIndexes(int tId, Number[] tuple) {
        for (int columnIdx = 0; columnIdx < tuple.length; columnIdx++) {
            Map<? extends Number, TIdSet> map = columnValues2Tid.get(columnIdx);
            if (map instanceof LTAggregateMap) {
                ((LTAggregateMap) map).addAndCreateIfNecessary(tuple[columnIdx], tId);
            } else if (map instanceof Float2ObjectOpenHashMap) {
                ((Float2ObjectOpenHashMap<TIdSet>) columnValues2Tid.get(columnIdx)).computeIfAbsent((float) tuple[columnIdx], k -> Utils.createNewTIdSet()).add(tId);
            } else {
                ((Int2ObjectOpenHashMap<TIdSet>) columnValues2Tid.get(columnIdx)).computeIfAbsent((int) tuple[columnIdx], k -> Utils.createNewTIdSet()).add(tId);
            }
        }
    }

    private boolean processPredicate(ScheduledPredicate predicate, Number[] tuple, Schedule schedule, int predIdx) {
        if (predicate.execute()) return true;

        // Utils.numPredicates++;
        Operand operand = getOperandsFor(predicate, tuple);
        for (var denialConstraint : predicate.belongsTo) {
            denialConstraint.applyOn(operand, predicate.op, allIds, predIdx);
        }
        if (predicate.tPrimePredicate != null) {
            for (var denialConstraint : predicate.tPrimePredicate.belongsTo) {
                denialConstraint.applyOn(operand, predicate.tPrimePredicate.op, allIds, schedule.idxMapping[predIdx]);
            }
        }
        return false;
    }

    @Override
    void validateTId(int tId, Number[] tuple) {
        batch.add(new IdTuplePair(tId, tuple));
        if (batch.size() == Utils.batchSize) {
            int firstOrderedCol = findFirstOrderedCol();
            if (firstOrderedCol != -1) {
                boolean isInt = tuple[firstOrderedCol] instanceof Integer;
                batch.sort((a, b) -> isInt ? Integer.compare((Integer) a.tuple[firstOrderedCol], (Integer) b.tuple[firstOrderedCol]) : Float.compare((Float) a.tuple[firstOrderedCol], (Float) b.tuple[firstOrderedCol]));
            }
            for (var pair : batch) {
                validateSingleTuple(pair.tId, pair.tuple);
            }
            batch.clear();
        }
    }

    private int findFirstOrderedCol() {
        for (var pred : schedule[0].predicates) {
            if (!pred.isSingleMapAccessible()) {
                return pred.probeColumnIdx;
            }
        }
        return -1;
    }

    public void reschedule() {
        for (int i = 0; i < columnValues2Tid.size(); i++) {
            var map = columnValues2Tid.get(i);
            columnCardinalities[i] = map.size();
        }
        schedule = predicateScheduler.schedulePredicates(columnCardinalities);
    }
}
