package de.hpi.isg;

import de.hpi.isg.schedules.*;

import java.util.*;

public class PredicateScheduler {
    public enum SortOrder {selectivityOnly, frequencyOnly, quotient, random}

    ArrayList<ArrayList<ScheduledPredicate>> tPredicatesInClasses = new ArrayList<>(6);
    ArrayList<ArrayList<ScheduledPredicate>> tPrimePredicatesInClasses = new ArrayList<>(3);
    ArrayList<ScheduledDenialConstraint> scheduledTDcs;
    ArrayList<ScheduledDenialConstraint> scheduledTPrimeDcs;

    ArrayList<ScheduledPredicate> allTPrimePredicates;
    ArrayList<ScheduledPredicate> allTPredicates;
    HashMap<Predicate, ScheduledPredicate> allTPrimePredicatesSet = new HashMap<>();
    Schedule[] result = new Schedule[2];
    InClassPredicateSorter predicateSorter = new InClassPredicateSorter();
    boolean[] tEqualClasses = new boolean[]{true, true, false, false, false, false};
    boolean[] tPrimeEqualClasses = new boolean[]{true, false, false};

    public void initDataStructures(ArrayList<DenialConstraint> denialConstraints) {
        ArrayList<HashMap<Predicate, ArrayList<ScheduledDenialConstraint>>> predicates2DCs = new ArrayList<>(6);
        ArrayList<HashMap<Predicate, ArrayList<ScheduledDenialConstraint>>> predicate2PrimeDCs = new ArrayList<>(3);
        scheduledTDcs = new ArrayList<>(denialConstraints.size());
        scheduledTPrimeDcs = new ArrayList<>(denialConstraints.size());

        predicates2DCs.add(new HashMap<>()); // SameColumnEq
        predicates2DCs.add(new HashMap<>()); // DiffColumnEq
        predicates2DCs.add(new HashMap<>()); // SameColumnNoneq
        predicates2DCs.add(new HashMap<>()); // DiffColumnNoneq
        predicates2DCs.add(new HashMap<>()); // SameColumnIneq
        predicates2DCs.add(new HashMap<>()); // DiffColumnIneq

        // No special treatment for same column attributes because there is no benefit for tPrime case
        predicate2PrimeDCs.add(new HashMap<>()); // Eq
        predicate2PrimeDCs.add(new HashMap<>()); // Noneq
        predicate2PrimeDCs.add(new HashMap<>()); // Ineq

        HashSet<Predicate> uniqueTPredicates = new HashSet<>();
        HashSet<Predicate> uniqueTPrimePredicates = new HashSet<>();

        for (var dc : denialConstraints) {
            uniqueTPredicates.addAll(dc.predicates);
            uniqueTPrimePredicates.addAll(dc.predicates);
        }

        int dcIdx = 0;
        for (DenialConstraint dc : denialConstraints) {
            boolean isReflexive = true;
            ScheduledDenialConstraint scheduledDc = new ScheduledDenialConstraint(uniqueTPredicates.size(), dcIdx);
            scheduledTDcs.add(scheduledDc);
            for (Predicate p : dc.predicates) {
                if (!p.isReflexive() && isReflexive) {
                    isReflexive = false;
                }
                predicates2DCs.get(p.getClassIdx()).computeIfAbsent(p, a -> new ArrayList<>()).add(scheduledDc);
            }

            if (isReflexive) {
                scheduledDc.isReflexive = true;
                for (Predicate p : dc.predicates) {
                    int idx;
                    switch (p.op) {
                        case EQUAL:
                            idx = 0;
                            break;
                        case UNEQUAL:
                            idx = 1;
                            break;
                        default:
                            idx = 2;
                            break;
                    }
                    predicate2PrimeDCs.get(idx).putIfAbsent(p, new ArrayList<>());
                }
            } else {
                ScheduledDenialConstraint scheduledTPrimeDc = new ScheduledDenialConstraint(uniqueTPrimePredicates.size(), dcIdx);
                scheduledTPrimeDcs.add(scheduledTPrimeDc);

                for (Predicate p : dc.predicates) {
                    int idx;
                    switch (p.op) {
                        case EQUAL:
                            idx = 0;
                            break;
                        case UNEQUAL:
                            idx = 1;
                            break;
                        default:
                            idx = 2;
                            break;
                    }
                    predicate2PrimeDCs.get(idx).computeIfAbsent(p, a -> new ArrayList<>()).add(scheduledTPrimeDc);
                }
            }
            dcIdx++;
        }

        allTPrimePredicates = new ArrayList<>(Collections.nCopies(uniqueTPrimePredicates.size(), null));
        if (Utils.inverseSorting) {
            Collections.reverse(predicate2PrimeDCs);
        }
        for (var predicates : predicate2PrimeDCs) {
            ArrayList<ScheduledPredicate> classPredicates = new ArrayList<>(predicates.size());
            for (var entry : predicates.entrySet()) {
                var pred = new ScheduledPredicate(entry.getKey(), entry.getValue());
                classPredicates.add(pred);
                if (pred.isSingleColumnPredicate()) {
                    allTPrimePredicatesSet.put(pred, pred);
                }
            }
            tPrimePredicatesInClasses.add(classPredicates);
        }

        allTPredicates =  new ArrayList<>(Collections.nCopies(uniqueTPredicates.size(), null));
        if (Utils.inverseSorting) {
            Collections.reverse(predicates2DCs);
        }
        for (var predicates : predicates2DCs) {
            ArrayList<ScheduledPredicate> classPredicates = new ArrayList<>(predicates.size());
            for (var entry : predicates.entrySet()) {
                classPredicates.add(new ScheduledPredicate(entry.getKey(), entry.getValue(), allTPrimePredicatesSet.get(entry.getKey())));
            }
            tPredicatesInClasses.add(classPredicates);
        }
    }

    public void preparePredicatesForNextIteration(ArrayList<ScheduledPredicate> allPredicates) {
        for (var pred : allPredicates) {
            pred.unexecute();
        }
    }

    Schedule[] scheduleFirstTime(int[] columnCardinalities) {
        if (Utils.predicateIdx.length == 0) {
            allTPredicates.clear();
            allTPrimePredicates.clear();
            reschedulePredicates(scheduledTDcs, allTPredicates, tPredicatesInClasses, columnCardinalities);
            result[0] = findAndCombinePrefixes(scheduledTDcs, allTPredicates);
            reschedulePredicates(scheduledTPrimeDcs, allTPrimePredicates, tPrimePredicatesInClasses, columnCardinalities);
            result[1] = findAndCombinePrefixes(scheduledTPrimeDcs, allTPrimePredicates);
        } else {
            result[0] = initFixedPermutation(scheduledTDcs, allTPredicates, tPredicatesInClasses);
            result[1] = initFixedPermutation(scheduledTPrimeDcs, allTPrimePredicates, tPrimePredicatesInClasses);
        }
        createPredicateIdxMapping(result[0], result[1]);
        return result;
    }

    private Schedule initFixedPermutation(ArrayList<ScheduledDenialConstraint> scheduledDcs, ArrayList<ScheduledPredicate> allPredicates, ArrayList<ArrayList<ScheduledPredicate>> predicatesInClasses) {
        for (var classPredicates : predicatesInClasses) {
            for (var pred : classPredicates) {
                int idx = Utils.predicateIdx[pred.index];
                allPredicates.set(idx, pred);
                pred.belongsTo.addAll(pred.baseDcs);
                for (var dc : pred.baseDcs) {
                    dc.predicateIndices[idx] = true;
                }
            }
        }
        Collections.sort(scheduledDcs);
        return new Schedule(scheduledDcs, allPredicates);
    }

    Schedule[] schedulePredicates(int[] columnCardinalities) {
        preparePredicatesForNextIteration(allTPredicates);
        preparePredicatesForNextIteration(allTPrimePredicates);
        prepareIntermediateMaintainerForNextIteration(scheduledTDcs);
        prepareIntermediateMaintainerForNextIteration(scheduledTPrimeDcs);
        prepareIntermediateMaintainerForNextIteration(result[0].prefixes);
        prepareIntermediateMaintainerForNextIteration(result[1].prefixes);
        preparePrefixIndicesForNextIteration(result[0]);
        preparePrefixIndicesForNextIteration(result[1]);

        if (Utils.predicateIdx.length == 0) {
            result[0].timeToReschedule--;
            if (result[0].timeToReschedule <= 0) {
                result[0].timeToReschedule = isOldScheduleStillValid(columnCardinalities, tPredicatesInClasses, tEqualClasses);
            }

            result[1].timeToReschedule--;
            if (result[1].timeToReschedule <= 0) {
                result[1].timeToReschedule = isOldScheduleStillValid(columnCardinalities, tPrimePredicatesInClasses, tPrimeEqualClasses);
            }

            if (result[0].timeToReschedule < 0) {
                reschedulePredicates(scheduledTDcs, allTPredicates, tPredicatesInClasses, columnCardinalities);
                result[0] = findAndCombinePrefixes(scheduledTDcs, allTPredicates);
                createPredicateIdxMapping(result[0], result[1]);
            }

            if (result[1].timeToReschedule < 0) {
                reschedulePredicates(scheduledTPrimeDcs, allTPrimePredicates, tPrimePredicatesInClasses, columnCardinalities);
                result[1] = findAndCombinePrefixes(scheduledTPrimeDcs, allTPrimePredicates);
                createPredicateIdxMapping(result[0], result[1]);
            }
        }

        return result;
    }

    private void preparePrefixIndicesForNextIteration(Schedule schedule) {
        if (schedule.basePrefixIndices != null) {
            System.arraycopy(schedule.basePrefixIndices, 0, schedule.prefixIndices, 0, schedule.basePrefixIndices.length);
        }
    }

    public void prepareIntermediateMaintainerForNextIteration(ArrayList<? extends IntermediateMaintainer> intermediateMaintainers) {
        for (var intermediateMaintainer : intermediateMaintainers) {
            intermediateMaintainer.unfinish();
        }
    }

    private void createPredicateIdxMapping(Schedule tSchedule, Schedule tPrimeSchedule) {
        HashMap<ScheduledPredicate, Integer> pred2idx = new HashMap<>(tPrimeSchedule.predicates.size());
        int[] idxMapping = new int[tSchedule.predicates.size()];
        for (int i = 0; i < tPrimeSchedule.predicates.size(); i++) {
            pred2idx.put(tPrimeSchedule.predicates.get(i), i);
        }
        for (int i = 0; i < tSchedule.predicates.size(); i++) {
            var currPred = tSchedule.predicates.get(i);
            if (currPred.tPrimePredicate != null) {
                idxMapping[i] = pred2idx.get(currPred.tPrimePredicate);
            }
        }
        tSchedule.idxMapping = idxMapping;
    }

    public void reschedulePredicates(ArrayList<ScheduledDenialConstraint> scheduledDcs, ArrayList<ScheduledPredicate> allPredicates, ArrayList<ArrayList<ScheduledPredicate>> predicatesInClasses, int[] columnCardinalities) {
        for (var pred : allPredicates) {
            pred.belongsTo.clear();
            pred.belongsTo.addAll(pred.baseDcs);
        }
        allPredicates.clear();
        for (var scheduledDc : scheduledDcs) {
            Arrays.fill(scheduledDc.predicateIndices, false);
        }
        predicateSorter.columnCardinalities = columnCardinalities;
        for (var classPredicates : predicatesInClasses) {
            if (Utils.inverseSorting) {
                classPredicates.sort(Collections.reverseOrder(predicateSorter));
            } else {
                classPredicates.sort(predicateSorter);
            }
            addPredicatesToDcs(allPredicates, classPredicates);
        }
    }

    private static int isOldScheduleStillValid(int[] columnCardinalities, ArrayList<ArrayList<ScheduledPredicate>> predicatesInClasses, boolean[] equalClasses) {
        int smallestDiff = Integer.MAX_VALUE;
        boolean stillValid = true;

        for (int i = 0; i < predicatesInClasses.size(); i++) {
            var classPredicates = predicatesInClasses.get(i);
            if (equalClasses[i]) {
                for (int predIdx = 0; predIdx < classPredicates.size() - 1; predIdx++) {
                    var predA = classPredicates.get(predIdx);
                    var predB = classPredicates.get(predIdx + 1);
                    if (columnCardinalities[predA.probeColumnIdx] < columnCardinalities[predB.probeColumnIdx]) {
                        stillValid = false;
                        break;
                    } else {
                        int diff = columnCardinalities[predA.probeColumnIdx] - columnCardinalities[predB.probeColumnIdx];
                        if (diff < smallestDiff)
                            smallestDiff = diff;
                    }
                }
            } else {
                for (int predIdx = 0; predIdx < classPredicates.size() - 1; predIdx++) {
                    var predA = classPredicates.get(predIdx);
                    var predB = classPredicates.get(predIdx + 1);
                    if (columnCardinalities[predA.probeColumnIdx] > columnCardinalities[predB.probeColumnIdx]) {
                        stillValid = false;
                        break;
                    } else {
                        int diff = columnCardinalities[predB.probeColumnIdx] - columnCardinalities[predA.probeColumnIdx];
                        if (diff < smallestDiff)
                            smallestDiff = diff;
                    }
                }
            }
        }

        if (stillValid)
            return smallestDiff;
        else
            return -1;
    }

    private static Schedule findAndCombinePrefixes(ArrayList<ScheduledDenialConstraint> scheduledDcs, ArrayList<ScheduledPredicate> allPredicates) {
        Collections.sort(scheduledDcs);
        if (!Utils.combinePrefix || scheduledDcs.size() <= 1) {
            return new Schedule(scheduledDcs, allPredicates);
        }

        HashSet<PossiblePrefix> possiblePrefixes = new HashSet<>();
        ArrayList<HashSet<IntermediateMaintainer>> predicate2Optimized = new ArrayList<>(allPredicates.size());
        for (int predIdx = 0; predIdx < allPredicates.size(); predIdx++) {
            predicate2Optimized.add(new HashSet<>());
        }

        for (var currDc : scheduledDcs) {
            HashSet<IntermediateMaintainer> activeOverlaps = null;
            boolean isFirstPred = true;
            int lastPredIdx = 0;
            PossiblePrefix possiblePrefix = new PossiblePrefix(currDc);

            for (int predIdx = currDc.getNextActivePredicate(0); predIdx != -1; predIdx = currDc.getNextActivePredicate(predIdx + 1)) {
                var pred = allPredicates.get(predIdx);
                if (activeOverlaps == null) {
                    activeOverlaps = new HashSet<>(pred.belongsTo);
                } else {
                    activeOverlaps.retainAll(pred.belongsTo);

                    if (activeOverlaps.size() == 1) {
                        break;
                    } else if (isFirstPred) {
                        isFirstPred = false;
                        possiblePrefix.stillPartOf[lastPredIdx] = (HashSet<IntermediateMaintainer>) activeOverlaps.clone();
                        predicate2Optimized.get(lastPredIdx).addAll(activeOverlaps);
                    }
                    possiblePrefix.stillPartOf[predIdx] = (HashSet<IntermediateMaintainer>) activeOverlaps.clone();
                    predicate2Optimized.get(predIdx).addAll(activeOverlaps);
                }
                lastPredIdx = predIdx;
            }

            if (!isFirstPred) {
                possiblePrefix.lastPredIdx = lastPredIdx;
                possiblePrefixes.add(possiblePrefix);
            }
        }
        ArrayList<PossiblePrefix> usedPrefixes = new ArrayList<>(possiblePrefixes);
        Collections.sort(usedPrefixes);
        ArrayList<PossiblePrefix> finalPrefixes = new ArrayList<>(possiblePrefixes.size());
        int[] prefixIndices = new int[allPredicates.size()];

        for (int prefixIdx = 0; prefixIdx < usedPrefixes.size(); prefixIdx++) {
            var prefix = usedPrefixes.get(prefixIdx);
            int use = 0, optimizationPotential = 0;
            for (int predIdx = prefix.owner.getNextActivePredicate(0); predIdx != -1; predIdx = prefix.owner.getNextActivePredicate(predIdx + 1)) {
                if (prefix.stillPartOf[predIdx] != null) {
                    use += prefix.stillPartOf[predIdx].size() - 1;
                }
                if (predicate2Optimized.get(predIdx).contains(prefix.owner)) {
                    optimizationPotential++;
                }
            }
            if (optimizationPotential > 0) {
                optimizationPotential--;
            }
            use -= prefix.stillPartOf[prefix.getNextActivePredicate(0)].size() - 1;

            if (use > optimizationPotential) {
                // update other prefixes
                finalPrefixes.add(prefix);
                var firstStillPartOf = prefix.stillPartOf[prefix.getNextActivePredicate(0)];
                for (int otherPrefixIdx = prefixIdx + 1; otherPrefixIdx < possiblePrefixes.size(); otherPrefixIdx++) {
                    var otherPrefix = usedPrefixes.get(otherPrefixIdx);
                    boolean isInPrefix = true;
                    for (int predIdx = otherPrefix.getNextActivePredicate(0); predIdx != -1; predIdx = otherPrefix.getNextActivePredicate(predIdx + 1)) {
                        if (prefix.owner.predicateIndices[predIdx]) {
                            if (!otherPrefix.stillPartOf[predIdx].containsAll(firstStillPartOf)) {
                                isInPrefix = false;
                            }
                            if (isInPrefix) {
                                if (otherPrefix.subsetOf[predIdx] == null) {
                                    otherPrefix.subsetOf[predIdx] = new HashSet<>(2);
                                } else {
                                    otherPrefix.subsetOf[predIdx].removeAll(prefix.subsetOf[predIdx]);
                                }
                                otherPrefix.subsetOf[predIdx].add(prefix);
                                predicate2Optimized.get(predIdx).remove(otherPrefix.owner);
                            }
                            otherPrefix.stillPartOf[predIdx].remove(prefix.owner);
                            if (prefix.stillPartOf[predIdx] != null) {
                                otherPrefix.stillPartOf[predIdx].removeAll(prefix.stillPartOf[predIdx]);
                            }
                        } else if (isInPrefix) {
                            isInPrefix = false;
                        }
                    }
                }
            }
        }

        ArrayList<ScheduledPrefix> scheduledPrefixes = new ArrayList<>(finalPrefixes.size());

        for (var prefix : finalPrefixes) {
            int lastPredIdx = -1;
            ScheduledPrefix scheduledPrefix = new ScheduledPrefix(prefix, prefixIndices);
            prefix.scheduledPrefix = scheduledPrefix;
            for (int predIdx = prefix.getNextActivePredicate(0); predIdx != -1; predIdx = prefix.getNextActivePredicate(predIdx + 1)) {
                if (prefix.subsetOf[predIdx] != null) {
                    for (var superset : prefix.subsetOf[predIdx]) {
                        prefix.stillPartOf[predIdx].add(superset.scheduledPrefix);
                    }
                }
                var currPartOf = prefix.stillPartOf[predIdx];
                prefixIndices[predIdx] += 1;
                if (currPartOf.isEmpty()) {
                    break;
                }

                scheduledPrefix.stillPartOf[predIdx] = currPartOf.toArray(new IntermediateMaintainer[0]);
                scheduledPrefix.predicateIndices[predIdx] = true;
                allPredicates.get(predIdx).belongsTo.removeAll(currPartOf);
                for (var other : currPartOf) {
//                    other.predicateIndices[predIdx] = false;
                }
                allPredicates.get(predIdx).belongsTo.add(scheduledPrefix);
                if (lastPredIdx != -1) {
                    var endOfOverlap = (HashSet<ScheduledDenialConstraint>) prefix.stillPartOf[lastPredIdx].clone();
                    endOfOverlap.removeAll(currPartOf);
                    scheduledPrefix.endOf[lastPredIdx] = endOfOverlap.toArray(new IntermediateMaintainer[0]);
                }
                lastPredIdx = predIdx;
            }
            if (lastPredIdx != -1) {
                scheduledPrefix.endOf[lastPredIdx] = scheduledPrefix.stillPartOf[lastPredIdx];
            }
            scheduledPrefixes.add(scheduledPrefix);
        }

        return new Schedule(scheduledDcs, allPredicates, scheduledPrefixes, prefixIndices);
    }

    private static void addPredicatesToDcs(ArrayList<ScheduledPredicate> allPredicates, ArrayList<ScheduledPredicate> classPredicates) {
        for (int idx = 0; idx < classPredicates.size(); idx++) {
            var p = classPredicates.get(idx);
            for (var dc : p.belongsTo) {
                dc.predicateIndices[idx + allPredicates.size()] = true;
            }
        }
        allPredicates.addAll(classPredicates);
    }

    private static class InClassPredicateSorter implements Comparator<ScheduledPredicate> {
        public int[] columnCardinalities;

        private final SortOrder sortOrder = Utils.sortOrder;

        @Override
        public int compare(ScheduledPredicate p0, ScheduledPredicate p1) {
            // Use p0 on right side to sort DESC
            switch (sortOrder) {
                case selectivityOnly: {
                    if (p0.op == Operator.EQUAL) {
                        return columnCardinalities[p1.probeColumnIdx] - columnCardinalities[p0.probeColumnIdx];
                    } else {
                        return columnCardinalities[p0.probeColumnIdx] - columnCardinalities[p1.probeColumnIdx];
                    }
                }
                case frequencyOnly: {
                    return p1.belongsTo.size() - p0.belongsTo.size();
                }
                case quotient:
                    if (p0.op == Operator.EQUAL) {
                        return Double.compare((double) p1.belongsTo.size() / columnCardinalities[p1.probeColumnIdx], (double) p0.belongsTo.size() / columnCardinalities[p0.probeColumnIdx]);
                    } else {
                        return Double.compare((double) p0.belongsTo.size() / columnCardinalities[p0.probeColumnIdx], (double) p1.belongsTo.size() / columnCardinalities[p1.probeColumnIdx]);
                    }
                default:
                    return p1.hashCode() - p0.hashCode();
            }
        }
    }
}

