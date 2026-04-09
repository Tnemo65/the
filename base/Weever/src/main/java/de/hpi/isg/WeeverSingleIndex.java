package de.hpi.isg;

import de.hpi.isg.tidSets.TIdSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;

public class WeeverSingleIndex extends Weever {
    ArrayList<Number[]> tuples = new ArrayList<>();
    Int2ObjectOpenHashMap<TIdSet> index = new Int2ObjectOpenHashMap<>();
    int indexColumnIdx;
    Predicate indexedPredicate;
    Predicate[] otherPredicates;

    public WeeverSingleIndex(ArrayList<DenialConstraint> denialConstraints, int[] keyColumns, ArrayList<String> columnTypes) {
        super(denialConstraints, keyColumns, columnTypes);

        for (var p : denialConstraints.get(0).predicates) {
            if (p.isSingleColumnPredicate() && p.op == Operator.EQUAL && !columnTypes.get(p.probeColumnIdx).equals("FLOAT")) {
                indexColumnIdx = p.probeColumnIdx;
                indexedPredicate = p;
                break;
            }
        }
        otherPredicates = new Predicate[denialConstraints.get(0).predicates.size() - 1];
        int i = 0;
        for (var p : denialConstraints.get(0).predicates) {
            if (p != indexedPredicate) {
                otherPredicates[i++] = p;
            }
        }
    }

    @Override
    void validateTId(int tId, Number[] intTuple) {
        TIdSet candidates = index.get((int) intTuple[indexColumnIdx]);
        if (candidates != null) {
            for (int candidate : candidates) {
                Number[] otherTuple = tuples.get(candidate);
                boolean isLeftViolation = validateTuple(intTuple, otherTuple);
                boolean isRightViolation = validateTuple(otherTuple, intTuple);

                if (isLeftViolation) {
                    finalTMatches.get(0).computeIfAbsent(tId, a -> Utils.createNewTIdSet()).add(candidate);
                }
                if (isRightViolation) {
                    finalTPrimeMatches.get(0).computeIfAbsent(tId, a -> Utils.createNewTIdSet()).add(candidate);
                }
            }
        }
        index.computeIfAbsent((int) intTuple[indexColumnIdx], k -> Utils.createNewTIdSet()).add(tId);
        tuples.add(intTuple);
    }

    private boolean validateTuple(Number[] intTuple, Number[] otherTuple) {
        for (var p: otherPredicates) {
            switch (p.op) {
                case EQUAL: if (!intTuple[p.probeColumnIdx].equals(otherTuple[p.queryColumnIdx])) return false; break;
                case UNEQUAL: if (intTuple[p.probeColumnIdx].equals(otherTuple[p.queryColumnIdx])) return false; break;
                case LESS: {
                    if (intTuple[p.probeColumnIdx] instanceof Float) {
                        var a = (float) intTuple[p.probeColumnIdx];
                        var b = (float) otherTuple[p.queryColumnIdx];
                        if (a >= b) return false;
                    } else {
                        var a = (int) intTuple[p.probeColumnIdx];
                        var b = (int) otherTuple[p.queryColumnIdx];
                        if (a >= b) return false;
                    }
                    break;
                }
                case GREATER: {
                    if (intTuple[p.probeColumnIdx] instanceof Float) {
                        var a = (float) intTuple[p.probeColumnIdx];
                        var b = (float) otherTuple[p.queryColumnIdx];
                        if (a <= b) return false;
                    } else {
                        var a = (int) intTuple[p.probeColumnIdx];
                        var b = (int) otherTuple[p.queryColumnIdx];
                        if (a <= b) return false;
                    }
                    break;
                }
            }
        }
        return true;
    }
}
