package de.hpi.isg;

import de.hpi.isg.dataStructures.ltAggregateMap.Float2TidSetLTTreeAggregateMap;
import de.hpi.isg.dataStructures.ltAggregateMap.Int2TidSetLTTreeAggregateMap;
import de.hpi.isg.tidSets.TIdSet;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.*;

public abstract class Weever {
    static class IdTuplePair {
        public int tId;
        public Number[] tuple;

        public IdTuplePair(int tId, Number[] tuple) {
            this.tId = tId;
            this.tuple = tuple;
        }
    }

    final ArrayList<DenialConstraint> denialConstraints;
    final int[] usedColumns;
    final ArrayList<String> columnTypes;
    final ArrayList<Map<? extends Number, TIdSet>> columnValues2Tid;
    final ArrayList<Int2ObjectOpenHashMap<TIdSet>> finalTMatches;
    final ArrayList<Int2ObjectOpenHashMap<TIdSet>> finalTPrimeMatches;
    int maxTid = 0;
    final HashMap<ArrayList<Integer>, Integer> key2Tid = new HashMap<>();
    final int[] keyColumns;
    // all Ids except currently inserted
    TIdSet allIds = Utils.createNewTIdSet();
    final Float2IntOpenHashMap floatDictionary = new Float2IntOpenHashMap();
    int lastFloatIndex = 0;
    final Object2IntOpenHashMap<String> stringDictionary = new Object2IntOpenHashMap<>();
    int lastStringIndex = 0;
    boolean deleteOccurred = false;
    final ArrayList<IdTuplePair> batch = new ArrayList<>(Utils.batchSize);

    public Weever(ArrayList<DenialConstraint> denialConstraints, int[] keyColumns, ArrayList<String> columnTypes) {
        this.denialConstraints = denialConstraints;
        this.keyColumns = keyColumns;
        this.columnTypes = columnTypes;

        finalTMatches = new ArrayList<>(denialConstraints.size());
        finalTPrimeMatches = new ArrayList<>(denialConstraints.size());

        for (int i = 0; i < denialConstraints.size(); i++) {
            finalTMatches.add(new Int2ObjectOpenHashMap<>());
            finalTPrimeMatches.add(new Int2ObjectOpenHashMap<>());
        }

        TreeSet<Integer> usedColumns = new TreeSet<>();
        HashSet<Integer> orderedColumns = new HashSet<>();
        for (DenialConstraint constraint : denialConstraints) {
            for (Predicate p : constraint.predicates) {
                if (!p.isSingleMapAccessible()) {
                    orderedColumns.add(p.probeColumnIdx);
                    orderedColumns.add(p.queryColumnIdx);
                }
                usedColumns.add(p.probeColumnIdx);
                usedColumns.add(p.queryColumnIdx);
            }
        }

        this.usedColumns = new int[usedColumns.size()];
        columnValues2Tid = new ArrayList<>(usedColumns.size());
        HashMap<Integer, Integer> col2newIdx = new HashMap<>();
        int i = 0;
        for (Integer usedColumn : usedColumns) {
            boolean isFloatType = columnTypes.get(usedColumn).equals("FLOAT");
            boolean isOrdered = orderedColumns.contains(usedColumn);
            columnValues2Tid.add(isFloatType ? isOrdered ? new Float2TidSetLTTreeAggregateMap() : new Float2ObjectOpenHashMap<>() : isOrdered ? new Int2TidSetLTTreeAggregateMap() : new Int2ObjectOpenHashMap<>());
            col2newIdx.put(usedColumn, i);
            this.usedColumns[i] = usedColumn;

            i++;
        }

        for (DenialConstraint constraint : denialConstraints) {
            ArrayList<Predicate> newPredicates = new ArrayList<>(constraint.predicates.size());
            for (Predicate p : constraint.predicates) {
                newPredicates.add(new Predicate(col2newIdx.get(p.probeColumnIdx), col2newIdx.get(p.queryColumnIdx), p.op, p.index));
            }
            constraint.predicates = newPredicates;
        }
    }

    private ArrayList<Integer> getKeyFromTuple(Object[] tuple) {
        ArrayList<Integer> key = new ArrayList<>(keyColumns.length);
        for (int keyColumn : keyColumns) {
            switch (columnTypes.get(keyColumn)) {
                case "INTEGER":
                    key.add((Integer) tuple[keyColumn]);
                    break;
                case "FLOAT":
                    key.add(floatDictionary.computeIfAbsent((float) tuple[keyColumn], l -> lastFloatIndex++));
                    break;
                default:
                    key.add(stringDictionary.computeIfAbsent((String) tuple[keyColumn], l -> lastStringIndex++));
            }
        }
        return key;
    }

    private int createNewKey(Object[] tuple) {
        if (Utils.storeViolations) {
            key2Tid.put(getKeyFromTuple(tuple), maxTid++);
        } else {
            ++maxTid;
        }
        return maxTid - 1;
    }

    private int getIdFromKey(Object[] tuple) {
        return key2Tid.get(getKeyFromTuple(tuple));
    }

    public void insert(Object[] tuple) {
        int tId = createNewKey(tuple);
        Number[] intTuple = getUsedTuple(tuple);
        validateTId(tId, intTuple);
    }

    abstract void validateTId(int tId, Number[] intTuple);

    private Number[] getUsedTuple(Object[] tuple) {
        Number[] intTuple = new Number[usedColumns.length];
        int idx = 0;
        for (int usedColumn : usedColumns) {
            switch (columnTypes.get(usedColumn)) {
                case "INTEGER":
                    intTuple[idx] = (Integer) tuple[usedColumn];
                    break;
                case "FLOAT":
                    intTuple[idx] = (Float) tuple[usedColumn];
                    break;
                default:
                    intTuple[idx] = stringDictionary.computeIfAbsent((String) tuple[usedColumn], l -> lastStringIndex++);
            }
            idx++;
        }
        return intTuple;
    }

    public void delete(Object[] tuple) {
        int tId = getIdFromKey(tuple);
        Number[] intTuple = getUsedTuple(tuple);
        deleteValuesFromMap(tId, intTuple);
        deleteFinalMatches(tId, finalTMatches);
        deleteFinalMatches(tId, finalTPrimeMatches);
        allIds.remove(tId);
        deleteOccurred = true;
    }

    private void deleteValuesFromMap(int tId, Number[] tuple) {
        // O(c)
        for (int columnIdx = 0; columnIdx < tuple.length; columnIdx++) {
            Map<?, TIdSet> map = columnValues2Tid.get(columnIdx);
            if (map instanceof Int2TidSetLTTreeAggregateMap) {
                ((Int2TidSetLTTreeAggregateMap) map).removeTIdAndSetIfNecessary((Integer) tuple[columnIdx], tId);
            } else if (map instanceof Float2TidSetLTTreeAggregateMap) {
                ((Float2TidSetLTTreeAggregateMap) map).removeTIdAndSetIfNecessary((Float) tuple[columnIdx], tId);
            } else {
                TIdSet equalVals = map.get(tuple[columnIdx]);
                equalVals.remove(tId);
                if (equalVals.isEmpty()) {
                    map.remove(tuple[columnIdx]);
                }
            }

        }
    }

    private void deleteFinalMatches(int tId, ArrayList<Int2ObjectOpenHashMap<TIdSet>> ownSideFinalMatches) {
        for (Int2ObjectOpenHashMap<TIdSet> ownSideFinalMatch : ownSideFinalMatches) {
            ownSideFinalMatch.remove(tId);
        }
    }

    public int[] getNumberOfViolations() {
        int[] result = new int[denialConstraints.size()];

        for (int dcIdx = 0; dcIdx < denialConstraints.size(); dcIdx++) {
            Int2ObjectOpenHashMap<TIdSet> leftSide = finalTMatches.get(dcIdx);
            int sum = getSumOfViolationsForOneSide(leftSide);
            Int2ObjectOpenHashMap<TIdSet> rightSide = finalTPrimeMatches.get(dcIdx);
            sum += getSumOfViolationsForOneSide(rightSide);
            result[dcIdx] = sum;
        }
        deleteOccurred = false;
        return result;
    }

    private int getSumOfViolationsForOneSide(Int2ObjectOpenHashMap<TIdSet> map) {
        int sum = 0;
        Iterator<Int2ObjectMap.Entry<TIdSet>> mapIter = map.int2ObjectEntrySet().iterator();
        while (mapIter.hasNext()) {
            var id2Matches = mapIter.next();
            TIdSet matches = id2Matches.getValue();
            if (deleteOccurred) {
                matches.intersect(allIds);
                if (matches.isEmpty()) {
                    mapIter.remove();
                } else {
                    sum += matches.cardinality();
                }
            } else {
                sum += matches.cardinality();
            }
        }
        return sum;
    }

    public ArrayList<Int2ObjectOpenHashMap<TIdSet>> getViolations() {
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> result = new ArrayList<>(denialConstraints.size());

        for (int dcIdx = 0; dcIdx < denialConstraints.size(); dcIdx++) {
            result.add(new Int2ObjectOpenHashMap<>());
            Int2ObjectOpenHashMap<TIdSet> rightSide = finalTPrimeMatches.get(dcIdx);
            for (Int2ObjectMap.Entry<TIdSet> id2Matches : rightSide.int2ObjectEntrySet()) {
                TIdSet matches = id2Matches.getValue();
                matches.intersect(allIds);
                if (matches.isEmpty()) {
                    rightSide.remove(id2Matches.getIntKey());
                } else {
                    for (int match : matches) {
                        result.get(dcIdx).computeIfAbsent(match, l -> Utils.createNewTIdSet()).add(id2Matches.getIntKey());
                    }
                }
            }
            Int2ObjectOpenHashMap<TIdSet> leftSide = finalTMatches.get(dcIdx);
            for (Int2ObjectMap.Entry<TIdSet> id2Matches : leftSide.int2ObjectEntrySet()) {
                TIdSet matches = id2Matches.getValue();
                matches.intersect(allIds);
                if (matches.isEmpty()) {
                    leftSide.remove(id2Matches.getIntKey());
                } else {
                    TIdSet rightMatches = result.get(dcIdx).get(id2Matches.getIntKey());
                    if (rightMatches == null) {
                        result.get(dcIdx).put(id2Matches.getIntKey(), matches);
                    } else {
                        rightMatches.union(matches);
                    }
                }
            }
        }
        return result;
    }
}
